"""Tests for generated public reference pages."""

from __future__ import annotations

from pathlib import Path

import pytest

import portolan_cli
from scripts.generate_reference_docs import generated_pages

pytestmark = pytest.mark.unit


def test_reference_pages_match_the_shipped_public_exports() -> None:
    """Generated API members derive from the supported public exports only."""
    pages = generated_pages(Path.cwd())

    python_reference = pages[Path("docs/reference/python.md")]
    assert "portolan_cli.Catalog" in python_reference
    assert "portolan_cli.backends.VersioningBackend" in python_reference
    assert "portolan_cli.sync.push" not in python_reference


def test_checked_in_reference_pages_are_fresh() -> None:
    """Checked-in generated reference pages match the generator output exactly."""
    project_root = Path(__file__).resolve().parents[2]
    pages = generated_pages(project_root)

    for relative_path, expected in pages.items():
        assert (project_root / relative_path).read_text(encoding="utf-8") == expected


def test_configuration_reference_reports_the_shipped_defaults() -> None:
    """The generated configuration page reads its defaults from the dataclass."""
    from portolan_cli.conversion_config import VectorSettings

    page = generated_pages(Path.cwd())[Path("docs/reference/configuration.md")]
    defaults = VectorSettings()

    assert "| `sort` |" in page
    assert f"| `{defaults.sort}` | Row ordering method. |" in page
    assert f"| `{str(defaults.add_bbox).lower()}` | Add a bbox covering column. |" in page
    # A literal pipe inside a cell would break the table.
    for line in page.splitlines():
        if line.startswith("| `"):
            assert line.count("|") - line.count(r"\|") == 5, line


def _cli_page() -> str:
    """Return the generated CLI reference page."""
    return generated_pages(Path.cwd())[Path("docs/reference/cli.md")]


def test_cli_reference_anchors_match_the_toc_slug() -> None:
    """Every heading anchor equals the slug Python-Markdown derives for it.

    The generator writes the anchor itself rather than import markdown. This
    proves the two agree for every command that ships.
    """
    from markdown.extensions.toc import slugify

    from scripts.generate_reference_docs import _slug, _walk

    paths = [ctx.command_path for ctx, _ in _walk(portolan_cli.cli)]
    assert len(paths) == 40
    for path in paths:
        assert _slug(path) == slugify(path, "-"), path


def test_cli_reference_drops_clicks_no_rewrap_marker() -> None:
    """The page carries no no-rewrap marker in either of its two forms.

    The previous renderer removed a line only when it held the backspace
    character. This repo's docstrings carry the two-character form, so 47
    markers reached the built page.
    """
    page = _cli_page()

    assert "\\b" not in page
    assert "\b" not in page


def test_cli_reference_fences_every_no_rewrap_block() -> None:
    """Each no-rewrap marker in the Click tree produces one fenced block.

    Click marks a paragraph that it must not rewrap. The page holds that
    paragraph in a fenced block, so the example commands inside it keep their
    line breaks.
    """
    from scripts.generate_reference_docs import _VERBATIM_MARKERS, _walk

    expected = sum(
        line.strip() in _VERBATIM_MARKERS
        for ctx, _ in _walk(portolan_cli.cli)
        for line in (ctx.command.help or "").splitlines()
    )
    assert expected == 47

    # Usage adds one fence per command, and options add none.
    commands = len(list(_walk(portolan_cli.cli)))
    assert _cli_page().count("```text") == expected + commands


def test_cli_reference_documents_every_shipped_command() -> None:
    """The page holds one heading per visible command, and the bullet list agrees."""
    from scripts.generate_reference_docs import _slug, _walk

    page = _cli_page()
    paths = [ctx.command_path for ctx, _ in _walk(portolan_cli.cli)]

    for path in paths:
        assert f" {path} {{ #{_slug(path)} " in page, path

    bullets = [line for line in page.splitlines() if line.startswith("- `portolan ")]
    assert len(bullets) == len(paths) - 1


def test_cli_reference_tables_keep_one_pipe_per_column() -> None:
    """No table cell holds a raw pipe, which would end the cell early."""
    page = _cli_page()
    rows = [line for line in page.splitlines() if line.startswith("| `")]

    assert rows, "the page holds no table rows"
    for row in rows:
        columns = row.replace("&#x7C;", "").count("|")
        assert columns in (4, 5), row


def test_cli_reference_lists_arguments_click_cannot_describe() -> None:
    """The page tables the arguments that the previous renderer omitted.

    Click holds no help text for an argument, so the old table style dropped
    arguments entirely. A reader could not see that `portolan add` takes paths.
    """
    page = _cli_page()

    assert "| Name | Type | Required |" in page
    assert "| `PATHS...` | path | yes |" in page
    assert "| `[OUTPUT_DIR]` | path | no |" in page


def test_cli_reference_ignores_the_terminal_width(monkeypatch: pytest.MonkeyPatch) -> None:
    """The page does not change with the width of the window that generates it.

    Click derives its default help width from the terminal size. A checked-in
    page that read it would fail `--check` on any machine with a different
    window.
    """
    # COLUMNS is the variable shutil.get_terminal_size reads first, so this
    # drives Click the same way a real terminal does. Do not patch
    # shutil.get_terminal_size itself, because pytest calls it with a
    # fallback= keyword while it writes progress.
    before = _cli_page()

    monkeypatch.setenv("COLUMNS", "37")
    narrow = _cli_page()

    monkeypatch.setenv("COLUMNS", "200")
    wide = _cli_page()

    assert narrow == before
    assert wide == before


def test_generator_rejects_a_machine_specific_default() -> None:
    """The generator stops when an option default names a path or an address.

    Such a default ties the checked-in page to the machine that wrote it. The
    generator fails loudly rather than write a page that CI cannot reproduce.
    """
    import click

    from scripts.generate_reference_docs import _command_options

    @click.command()
    @click.option("--cache", default="/home/someone/.cache")
    def leaky() -> None:
        """Hold a machine-specific default."""

    ctx = click.Context(leaky, info_name="leaky")
    with pytest.raises(ValueError, match="absolute path"):
        _command_options(ctx)
