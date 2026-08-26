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
