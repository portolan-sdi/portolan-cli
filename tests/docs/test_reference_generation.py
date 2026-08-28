"""Tests for generated public reference pages."""

from __future__ import annotations

from pathlib import Path

import pytest

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
