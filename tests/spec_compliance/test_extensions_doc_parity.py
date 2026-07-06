"""Parity between spec/extensions.md and the code extension vocabulary (#558).

`spec/extensions.md` is the human-facing canonical doc; the code frozensets are
derived from `portolan_cli.extension_registry` (ADR-0055). This test parses the
markdown tables and fails if the doc and the code disagree, so the two can never
drift again.

Parsing model: for each level-2 (`##`) section, collect the backtick-quoted
tokens in the FIRST column of every markdown table row. That yields the set of
extensions each section documents.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from portolan_cli.constants import GEOSPATIAL_EXTENSIONS, TABULAR_EXTENSIONS
from portolan_cli.extension_registry import EXTENSION_REGISTRY, all_known_sidecar_extensions
from portolan_cli.formats import UNSUPPORTED_EXTENSIONS
from portolan_cli.scan_classify import (
    DOC_EXTENSIONS,
    IMAGE_EXTENSIONS,
    JUNK_DIRS,
    JUNK_EXTENSIONS,
    SIDECAR_EXTENSIONS,
    VIZ_EXTENSIONS,
)

pytestmark = pytest.mark.unit

_EXTENSIONS_MD = Path(__file__).resolve().parents[2] / "spec" / "extensions.md"
_BACKTICK = re.compile(r"`([^`]+)`")


def _section_first_column_tokens(text: str) -> dict[str, set[str]]:
    """Map each ``## Section`` title to the set of backticked first-column tokens."""
    sections: dict[str, set[str]] = {}
    current: str | None = None
    for line in text.splitlines():
        if line.startswith("## "):
            current = line[3:].strip()
            sections.setdefault(current, set())
            continue
        if current is None or not line.lstrip().startswith("|"):
            continue
        first_cell = line.split("|")[1] if line.count("|") >= 2 else ""
        sections[current].update(_BACKTICK.findall(first_cell))
    return sections


@pytest.fixture(scope="module")
def doc_sections() -> dict[str, set[str]]:
    return _section_first_column_tokens(_EXTENSIONS_MD.read_text(encoding="utf-8"))


def test_extensions_md_exists() -> None:
    assert _EXTENSIONS_MD.is_file()


def test_primary_geospatial_matches_code(doc_sections: dict[str, set[str]]) -> None:
    # Documented importable formats. `.pmtiles` is a geospatial format but is
    # documented under Visualization; `.json` is content-inspected and not in
    # GEOSPATIAL_EXTENSIONS. Both handled explicitly here.
    expected = (GEOSPATIAL_EXTENSIONS - {".pmtiles"}) | {".json"}
    assert doc_sections["Primary Geospatial Formats"] == expected


def test_additional_cloud_native_matches_code(doc_sections: dict[str, set[str]]) -> None:
    expected = {
        spec.ext
        for spec in EXTENSION_REGISTRY
        if spec.cloud_native == "yes" and (spec.is_dir or spec.is_compound)
    }
    assert doc_sections["Additional Cloud-Native Formats"] == expected == {".zarr", ".copc.laz"}


def test_visualization_matches_code(doc_sections: dict[str, set[str]]) -> None:
    assert doc_sections["Visualization Formats"] == VIZ_EXTENSIONS | {".pmtiles"}


def test_thumbnails_match_code(doc_sections: dict[str, set[str]]) -> None:
    assert doc_sections["Thumbnail/Preview"] == IMAGE_EXTENSIONS


def test_unsupported_matches_code(doc_sections: dict[str, set[str]]) -> None:
    assert doc_sections["Unsupported Formats"] == UNSUPPORTED_EXTENSIONS


def test_tabular_matches_code(doc_sections: dict[str, set[str]]) -> None:
    assert doc_sections["Tabular Formats"] == TABULAR_EXTENSIONS


def test_sidecars_are_documented_and_known(doc_sections: dict[str, set[str]]) -> None:
    documented = doc_sections["Sidecar Files"]
    # Every scanner-recognized sidecar must be documented, and every documented
    # sidecar must be a real known sidecar (the doc may curate a subset, e.g. it
    # omits obscure .img sidecars).
    assert SIDECAR_EXTENSIONS <= documented <= all_known_sidecar_extensions()


def test_ignored_files_match_code(doc_sections: dict[str, set[str]]) -> None:
    tokens = doc_sections["Ignored Files"]
    doc_dirs = {t[:-1] for t in tokens if t.endswith("/")}
    doc_exts = {t for t in tokens if t.startswith(".") and not t.endswith("/")}
    assert doc_exts == JUNK_EXTENSIONS | DOC_EXTENSIONS
    assert doc_dirs == JUNK_DIRS
