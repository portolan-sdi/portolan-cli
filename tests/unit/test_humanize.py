"""Unit tests for the slug humanizer (Issue #502).

Titles on collections, items, and child/item links must be human-readable.
These tests pin down how a machine slug like
``publico_areas_programaticas_desarrollo_social`` becomes
``Publico Areas Programaticas Desarrollo Social``.
"""

from __future__ import annotations

import pytest

from portolan_cli.humanize import derive_title, humanize_slug


@pytest.mark.unit
class TestHumanizeSlug:
    def test_issue_example_spanish_slug(self) -> None:
        assert (
            humanize_slug("publico_areas_programaticas_desarrollo_social")
            == "Publico Areas Programaticas Desarrollo Social"
        )

    def test_simple_snake_case(self) -> None:
        assert humanize_slug("arbolado_publico") == "Arbolado Publico"

    def test_hyphenated_slug(self) -> None:
        assert humanize_slug("land-use") == "Land Use"

    def test_mixed_separators(self) -> None:
        assert humanize_slug("land-use_2018") == "Land Use 2018"

    def test_numeric_token_preserved(self) -> None:
        assert humanize_slug("census_2020") == "Census 2020"

    def test_nested_id_uses_leaf_segment(self) -> None:
        assert humanize_slug("climate/hittekaart") == "Hittekaart"
        assert humanize_slug("rivers/2020/q1") == "Q1"

    def test_acronym_token_preserved(self) -> None:
        # A token that already contains uppercase is left untouched.
        assert humanize_slug("ign_layers") == "Ign Layers"
        assert humanize_slug("IGN_layers") == "IGN Layers"

    def test_camelcase_token_preserved(self) -> None:
        assert humanize_slug("DenHaagHousing") == "DenHaagHousing"

    def test_collapses_repeated_and_edge_separators(self) -> None:
        assert humanize_slug("__foo__bar__") == "Foo Bar"

    def test_empty_and_none(self) -> None:
        assert humanize_slug("") == ""
        assert humanize_slug(None) == ""  # type: ignore[arg-type]


@pytest.mark.unit
class TestDeriveTitle:
    def test_uses_existing_human_title(self) -> None:
        assert derive_title("Arbolado Público", "arbolado_publico") == "Arbolado Público"

    def test_humanizes_when_existing_is_technical(self) -> None:
        assert derive_title("arbolado_publico", "arbolado_publico") == "Arbolado Publico"

    def test_humanizes_when_existing_missing(self) -> None:
        assert derive_title(None, "arbolado_publico") == "Arbolado Publico"
        assert derive_title("", "climate/hittekaart") == "Hittekaart"

    def test_preserves_camelcase_existing(self) -> None:
        # CamelCase is considered human-readable by is_technical_name.
        assert derive_title("DenHaagHousing", "den_haag") == "DenHaagHousing"
