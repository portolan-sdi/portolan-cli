"""Tests for scripts/check_comments.py."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.check_comments import check, main, rendered_objects

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[2]


def _codes(
    tmp_path: Path, name: str, text: str, rendered: dict[str, set[str]] | None = None
) -> list[str]:
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return [line.split()[1] for line in check(path, tmp_path, rendered or {})]


def test_two_line_docstring_passes(tmp_path: Path) -> None:
    assert _codes(tmp_path, "a.py", '"""One.\n\nTwo.\n"""\n') == []


def test_three_line_docstring_fails(tmp_path: Path) -> None:
    src = 'def f() -> None:\n    """One.\n\n    Two.\n    Three.\n    """\n'
    assert _codes(tmp_path, "a.py", src) == ["DOC-LEN"]


def test_two_line_comment_block_passes(tmp_path: Path) -> None:
    assert _codes(tmp_path, "a.py", "# one\n# two\nx = 1\n# three\n") == []


def test_three_line_comment_block_fails(tmp_path: Path) -> None:
    assert _codes(tmp_path, "a.py", "x = 1\n    # one\n# two\n# three\n") == ["COM-LEN"]


def test_trailing_comments_never_join_a_block(tmp_path: Path) -> None:
    src = "# one\nx = 1  # noqa: E501 - long\ny = 2  # type: ignore[misc]\n# two\n"
    assert _codes(tmp_path, "a.py", src) == []


@pytest.mark.parametrize(
    "text",
    [
        "see #123",
        "this used to fail",
        "No longer needed",
        "previously cached",
        "formerly named x",
        "agent B owns it",
        "lane 2 work",
        "track A1 item",
        "added 2026-10-23",
        "since March 2026",
    ],
)
def test_history_is_banned(tmp_path: Path, text: str) -> None:
    assert _codes(tmp_path, "a.py", f"x = 1  # {text}\n") == ["COM-HIST"]
    assert _codes(tmp_path, "b.py", f'"""{text}."""\n') == ["DOC-HIST"]


@pytest.mark.parametrize("text", ["color &#123;", "a/#12 path", "track a file", "the C#5 note"])
def test_history_lookalikes_pass(tmp_path: Path, text: str) -> None:
    assert _codes(tmp_path, "a.py", f"# {text}\n") == []


def test_url_line_is_exempt_from_history(tmp_path: Path) -> None:
    src = "# upstream: https://github.com/o/r/issues/12 used to break (#12)\n"
    assert _codes(tmp_path, "a.py", src) == []


def test_hash_in_string_is_not_a_comment(tmp_path: Path) -> None:
    src = 'x = "# one"\ny = """\n# two\n# three\n# four #12\n"""\n'
    assert _codes(tmp_path, "a.py", src) == []


LONG = '"""One.\n\n    Two.\n    Three.\n    """'


@pytest.mark.parametrize(
    "decorator", ["@cli.command()", "@click.group()", '@extract.command("arcgis")']
)
def test_click_docstrings_are_exempt_from_length(tmp_path: Path, decorator: str) -> None:
    src = f"{decorator}\ndef cmd() -> None:\n    {LONG}\n"
    assert _codes(tmp_path, "a.py", src) == []


def test_click_docstrings_keep_the_history_ban(tmp_path: Path) -> None:
    src = '@cli.command()\ndef cmd() -> None:\n    """Run it (#12)."""\n'
    assert _codes(tmp_path, "a.py", src) == ["DOC-HIST"]


def test_click_docstrings_may_show_example_dates(tmp_path: Path) -> None:
    src = '@cli.command()\ndef cmd() -> None:\n    """Run it.\n\n    portolan add x --datetime 2024-06-15\n    """\n'
    assert _codes(tmp_path, "a.py", src) == []


def test_other_docstrings_keep_the_date_ban(tmp_path: Path) -> None:
    assert _codes(tmp_path, "a.py", 'def f() -> None:\n    """Fixed on 2024-06-15."""\n') == [
        "DOC-HIST"
    ]


def test_api_groups_docstring_is_exempt(tmp_path: Path) -> None:
    src = '"""API.\n\nGroups:\n\n- Hooks: x,\n  y.\n- More.\n"""\n'
    assert _codes(tmp_path, "portolan_cli/api.py", src) == []
    assert _codes(tmp_path, "portolan_cli/other.py", src) == ["DOC-LEN"]


RENDERED_CLASS = '''"""Module."""


class Thing:
    """A thing.

    Args:
        name: The name.

    Returns:
        Nothing.
    """

    def run(self, n: int) -> int:
        """Run it.

        Args:
            n: The count.
            m: Another count.

        Raises:
            ValueError: On a bad count.
        """
        return n
'''


def test_rendered_object_and_member_may_use_sections(tmp_path: Path) -> None:
    rendered = {"pkg/mod.py": {"Thing"}}
    assert _codes(tmp_path, "pkg/mod.py", RENDERED_CLASS, rendered) == []
    assert _codes(tmp_path, "pkg/mod.py", RENDERED_CLASS) == ["DOC-LEN", "DOC-LEN"]


def test_rendered_prose_paragraph_fails(tmp_path: Path) -> None:
    src = 'def f() -> None:\n    """One.\n\n    More prose.\n    And more.\n\n    Args:\n        x: X.\n    """\n'
    assert _codes(tmp_path, "m.py", src, {"m.py": {"f"}}) == ["DOC-LEN"]


def test_rendered_continuation_line_fails(tmp_path: Path) -> None:
    src = (
        'def f() -> None:\n    """One.\n\n    Args:\n        x: X\n            and more.\n    """\n'
    )
    assert _codes(tmp_path, "m.py", src, {"m.py": {"f"}}) == ["DOC-LEN"]


def test_missing_rendered_docstring_fails(tmp_path: Path) -> None:
    src = '"""Module."""\n\n\nclass Thing:\n    def run(self) -> None:\n        pass\n\n    def _private(self) -> None:\n        pass\n'
    assert _codes(tmp_path, "m.py", src, {"m.py": {"Thing"}}) == ["DOC-MISSING", "DOC-MISSING"]


def test_rendered_objects_resolve_through_reexports(tmp_path: Path) -> None:
    (tmp_path / "docs/reference").mkdir(parents=True)
    (tmp_path / "docs/reference/python.md").write_text(
        "::: pkg.Thing\n    options:\n      x: 1\n\n::: pkg.api.helper\n\n::: core.mod.Other\n",
        encoding="utf-8",
    )
    (tmp_path / "mkdocs.yml").write_text("          paths: [., src]\n", encoding="utf-8")
    (tmp_path / "pkg").mkdir()
    (tmp_path / "pkg/__init__.py").write_text(
        "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n    from pkg.mod import Thing\n__all__ = ['Thing']\n",
        encoding="utf-8",
    )
    (tmp_path / "pkg/mod.py").write_text("class Thing:\n    pass\n", encoding="utf-8")
    (tmp_path / "pkg/api.py").write_text("from .impl import helper as helper\n", encoding="utf-8")
    (tmp_path / "pkg/impl.py").write_text("def helper() -> None:\n    pass\n", encoding="utf-8")
    (tmp_path / "src/core").mkdir(parents=True)
    (tmp_path / "src/core/__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "src/core/mod.py").write_text("class Other:\n    pass\n", encoding="utf-8")
    assert rendered_objects(tmp_path) == {
        "pkg/mod.py": {"Thing"},
        "pkg/impl.py": {"helper"},
        "src/core/mod.py": {"Other"},
    }


def test_real_reference_page_resolves_every_object() -> None:
    rendered = rendered_objects(REPO_ROOT)
    assert sum(len(names) for names in rendered.values()) == 98
    assert "Catalog" in rendered["portolan_cli/catalog.py"]


def test_toml_and_yaml_count_only_comment_lines(tmp_path: Path) -> None:
    good = 'a = "# not a comment"\n# one\n# two\nb = 1  # trailing\n'
    assert _codes(tmp_path, "pyproject.toml", good) == []
    assert _codes(tmp_path, "prek.toml", "# one\n# two\n# three\n") == ["COM-LEN"]
    yml = "jobs:\n  # see #12\n  x: '# a'\n"
    assert _codes(tmp_path, ".github/workflows/ci.yml", yml) == ["COM-HIST"]


def test_files_synced_from_ops_are_skipped(tmp_path: Path) -> None:
    head = "# Thin caller — synced from portolan-sdi/portolan-ops (ci/x.yml).\n# Edit it there.\n"
    assert _codes(tmp_path, ".github/workflows/x.yml", head + "# a\n# b\n# see #12\n") == []


def test_main_ignores_other_paths_and_reports(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "notes.md").write_text("# one\n# two\n# three\n", encoding="utf-8")
    assert main([str(tmp_path / "notes.md")]) == 0
    bad = tmp_path / "a.py"
    bad.write_text("x = 1  # used to\n", encoding="utf-8")
    assert main([str(bad)]) == 1
    assert capsys.readouterr().out == f"{bad}:1: COM-HIST history in a comment\n"
