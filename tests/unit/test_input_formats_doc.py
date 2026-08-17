"""Freshness guard for the generated input-formats reference page.

``docs/reference/input-formats.md`` is generated from
``portolan_cli.extension_registry`` (the single source of the extension
vocabulary, ADR-0055) by ``scripts/gen_input_formats_doc.py``. This test fails
whenever the registry changes without regenerating the page, replacing the
hand-written extensions doc that lived in the removed vendored spec folder and
its parity test (#751): a generated doc cannot drift, provided it is
regenerated.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.extension_registry import EXTENSION_REGISTRY
from scripts.gen_input_formats_doc import render

pytestmark = pytest.mark.unit

_DOC = Path(__file__).resolve().parents[2] / "docs" / "reference" / "input-formats.md"


def test_checked_in_page_is_fresh() -> None:
    """The committed page equals the generator's output byte-for-byte."""
    assert _DOC.read_text(encoding="utf-8") == render(), (
        "docs/reference/input-formats.md is stale; regenerate it with "
        "`uv run python scripts/gen_input_formats_doc.py`"
    )


def test_render_covers_every_registry_extension() -> None:
    """Every registry row's extension appears in the rendered page."""
    rendered = render()
    missing = [spec.ext for spec in EXTENSION_REGISTRY if f"`{spec.ext}`" not in rendered]
    assert not missing, f"registry extensions absent from the page: {missing}"


def test_render_marks_the_page_as_generated() -> None:
    """The page carries a do-not-edit marker naming the generator."""
    assert "scripts/gen_input_formats_doc.py" in render()
