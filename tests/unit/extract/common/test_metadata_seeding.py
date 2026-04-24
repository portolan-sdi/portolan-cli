"""Tests for common metadata seeding utilities.

These tests verify the shared metadata seeding functionality used
by both WFS and ArcGIS extraction backends.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit


class TestIsTechnicalName:
    """Tests for _is_technical_name function."""

    def test_underscore_only_identifier(self) -> None:
        """Lowercase with underscores detected as technical."""
        from portolan_cli.extract.common.metadata_seeding import _is_technical_name

        assert _is_technical_name("bu_building_emprise") is True
        assert _is_technical_name("road_centerlines") is True
        assert _is_technical_name("census_2020") is True

    def test_namespace_prefix(self) -> None:
        """Namespace:name pattern detected as technical."""
        from portolan_cli.extract.common.metadata_seeding import _is_technical_name

        assert _is_technical_name("inspire_bu:BU.Building") is True
        assert _is_technical_name("ns:LayerName") is True

    def test_short_no_spaces(self) -> None:
        """Short strings without spaces are technical."""
        from portolan_cli.extract.common.metadata_seeding import _is_technical_name

        assert _is_technical_name("buildings") is True
        assert _is_technical_name("Layer-1") is True

    def test_real_description(self) -> None:
        """Real descriptions with spaces are not technical."""
        from portolan_cli.extract.common.metadata_seeding import _is_technical_name

        assert _is_technical_name("Building footprints for the region") is False
        assert _is_technical_name("Road centerlines with classification") is False
        assert _is_technical_name("Census blocks 2020") is False

    def test_title_with_dashes(self) -> None:
        """Titles with dashes and spaces are not technical."""
        from portolan_cli.extract.common.metadata_seeding import _is_technical_name

        assert _is_technical_name("Building - building_emprise") is False
        assert _is_technical_name("Roads - main network") is False

    def test_none_and_empty(self) -> None:
        """None and empty are treated as technical (unusable)."""
        from portolan_cli.extract.common.metadata_seeding import _is_technical_name

        assert _is_technical_name(None) is True
        assert _is_technical_name("") is True
        assert _is_technical_name("   ") is True


class TestSelectBestDescription:
    """Tests for _select_best_description function."""

    def test_prefers_real_abstract(self) -> None:
        """Uses abstract when it's a real description."""
        from portolan_cli.extract.common.metadata_seeding import _select_best_description

        result = _select_best_description(
            abstract="All building footprints in the study area",
            title="Buildings Layer",
            layer_name="buildings",
        )
        assert result == "All building footprints in the study area"

    def test_falls_back_to_title(self) -> None:
        """Falls back to title when abstract is technical."""
        from portolan_cli.extract.common.metadata_seeding import _select_best_description

        result = _select_best_description(
            abstract="bu_building_emprise",
            title="Building - building_emprise",
            layer_name="inspire_bu:BU.Building_building_emprise",
        )
        assert result == "Building - building_emprise"

    def test_prefers_title_with_spaces_over_underscore_abstract(self) -> None:
        """Title with spaces beats abstract with underscores."""
        from portolan_cli.extract.common.metadata_seeding import _select_best_description

        result = _select_best_description(
            abstract="road_network_2020",
            title="Road Network 2020",
            layer_name="roads",
        )
        # Title has spaces, abstract doesn't - prefer title
        assert result == "Road Network 2020"

    def test_none_abstract_uses_title(self) -> None:
        """When abstract is None, use title."""
        from portolan_cli.extract.common.metadata_seeding import _select_best_description

        result = _select_best_description(
            abstract=None,
            title="Census Blocks",
            layer_name="census",
        )
        assert result == "Census Blocks"

    def test_both_technical_prefers_title_with_spaces(self) -> None:
        """When both are short, prefer title if it has better format."""
        from portolan_cli.extract.common.metadata_seeding import _select_best_description

        result = _select_best_description(
            abstract="buildings",
            title="Buildings - Main",
            layer_name="buildings",
        )
        assert result == "Buildings - Main"

    def test_title_same_as_name_returns_abstract(self) -> None:
        """When title equals layer name, prefer abstract even if short."""
        from portolan_cli.extract.common.metadata_seeding import _select_best_description

        result = _select_best_description(
            abstract="building_footprints",
            title="buildings",
            layer_name="buildings",
        )
        # Title == layer_name, so we don't use it as description
        assert result == "building_footprints"


class TestSeedCollectionMetadata:
    """Tests for seed_collection_metadata function."""

    def test_creates_metadata_yaml_in_collection(self, tmp_path: Path) -> None:
        """Creates .portolan/metadata.yaml in collection directory."""
        from portolan_cli.extract.common.metadata_seeding import seed_collection_metadata

        collection_dir = tmp_path / "my_collection"
        collection_dir.mkdir()

        result = seed_collection_metadata(
            collection_dir,
            source_type="wfs",
            source_url="https://example.com/wfs?service=WFS&request=GetFeature&typename=buildings",
            layer_name="buildings",
            title="Buildings Layer",
            description="All buildings in the region",
        )

        assert result is True
        metadata_path = collection_dir / ".portolan" / "metadata.yaml"
        assert metadata_path.exists()

    def test_includes_layer_description(self, tmp_path: Path) -> None:
        """Description from layer abstract is included in seeded file."""
        from portolan_cli.extract.common.metadata_seeding import seed_collection_metadata

        collection_dir = tmp_path / "roads"
        collection_dir.mkdir()

        seed_collection_metadata(
            collection_dir,
            source_type="wfs",
            source_url="https://example.com/wfs",
            layer_name="roads",
            description="Road centerlines dataset with classification",
        )

        content = (collection_dir / ".portolan" / "metadata.yaml").read_text()
        assert "Road centerlines dataset with classification" in content

    def test_includes_layer_title_in_processing_notes(self, tmp_path: Path) -> None:
        """Layer title appears in processing_notes when different from name."""
        import yaml

        from portolan_cli.extract.common.metadata_seeding import seed_collection_metadata

        collection_dir = tmp_path / "census"
        collection_dir.mkdir()

        seed_collection_metadata(
            collection_dir,
            source_type="arcgis_featureserver",
            source_url="https://example.com/arcgis/rest/services/Census/FeatureServer/0",
            layer_name="Census_Blocks",
            title="Census Block Groups 2020",
            description="Census block group boundaries",
        )

        content = (collection_dir / ".portolan" / "metadata.yaml").read_text()
        metadata = yaml.safe_load(content)
        assert "Census_Blocks" in metadata["processing_notes"]
        assert "Census Block Groups 2020" in metadata["processing_notes"]

    def test_skips_existing_metadata_yaml(self, tmp_path: Path) -> None:
        """Does not overwrite existing metadata.yaml."""
        from portolan_cli.extract.common.metadata_seeding import seed_collection_metadata

        collection_dir = tmp_path / "existing"
        collection_dir.mkdir()
        portolan_dir = collection_dir / ".portolan"
        portolan_dir.mkdir()
        existing_content = "# User customized\nlicense: CC-BY-4.0\n"
        (portolan_dir / "metadata.yaml").write_text(existing_content)

        result = seed_collection_metadata(
            collection_dir,
            source_type="wfs",
            source_url="https://example.com/wfs",
            layer_name="test",
            description="This should not appear",
        )

        assert result is False
        content = (portolan_dir / "metadata.yaml").read_text()
        assert "User customized" in content
        assert "This should not appear" not in content

    def test_handles_none_description(self, tmp_path: Path) -> None:
        """Works when description is None."""
        from portolan_cli.extract.common.metadata_seeding import seed_collection_metadata

        collection_dir = tmp_path / "no_desc"
        collection_dir.mkdir()

        result = seed_collection_metadata(
            collection_dir,
            source_type="wfs",
            source_url="https://example.com/wfs",
            layer_name="unnamed_layer",
            title=None,
            description=None,
        )

        assert result is True
        metadata_path = collection_dir / ".portolan" / "metadata.yaml"
        assert metadata_path.exists()
        content = metadata_path.read_text()
        assert "source_url" in content
        assert "processing_notes" in content

    def test_handles_none_title(self, tmp_path: Path) -> None:
        """Works when title is None - doesn't add title to processing_notes."""
        from portolan_cli.extract.common.metadata_seeding import seed_collection_metadata

        collection_dir = tmp_path / "no_title"
        collection_dir.mkdir()

        seed_collection_metadata(
            collection_dir,
            source_type="wfs",
            source_url="https://example.com/wfs",
            layer_name="my_layer",
            title=None,
            description="Has description but no title",
        )

        content = (collection_dir / ".portolan" / "metadata.yaml").read_text()
        assert "my_layer" in content
        assert "Has description but no title" in content

    def test_title_same_as_name_not_duplicated(self, tmp_path: Path) -> None:
        """When title equals layer_name, don't duplicate in processing_notes."""
        from portolan_cli.extract.common.metadata_seeding import seed_collection_metadata

        collection_dir = tmp_path / "same_name"
        collection_dir.mkdir()

        seed_collection_metadata(
            collection_dir,
            source_type="wfs",
            source_url="https://example.com/wfs",
            layer_name="buildings",
            title="buildings",  # Same as layer_name
            description="Some buildings",
        )

        content = (collection_dir / ".portolan" / "metadata.yaml").read_text()
        # Should not have "(buildings)" appended since title == name
        assert "Extracted from wfs layer: buildings" in content
        assert "(buildings)" not in content

    def test_includes_source_url(self, tmp_path: Path) -> None:
        """Source URL is included in seeded metadata."""
        from portolan_cli.extract.common.metadata_seeding import seed_collection_metadata

        collection_dir = tmp_path / "with_url"
        collection_dir.mkdir()

        seed_collection_metadata(
            collection_dir,
            source_type="arcgis_featureserver",
            source_url="https://services.arcgis.com/abc/FeatureServer/0",
            layer_name="test",
        )

        content = (collection_dir / ".portolan" / "metadata.yaml").read_text()
        assert "https://services.arcgis.com/abc/FeatureServer/0" in content

    def test_includes_keywords_when_provided(self, tmp_path: Path) -> None:
        """Keywords are included when provided."""
        from portolan_cli.extract.common.metadata_seeding import seed_collection_metadata

        collection_dir = tmp_path / "with_keywords"
        collection_dir.mkdir()

        seed_collection_metadata(
            collection_dir,
            source_type="wfs",
            source_url="https://example.com/wfs",
            layer_name="parks",
            keywords=["recreation", "green space", "urban"],
        )

        content = (collection_dir / ".portolan" / "metadata.yaml").read_text()
        assert "recreation" in content
        assert "green space" in content
        assert "urban" in content

    def test_creates_portolan_dir_if_missing(self, tmp_path: Path) -> None:
        """Creates .portolan directory if it doesn't exist."""
        from portolan_cli.extract.common.metadata_seeding import seed_collection_metadata

        collection_dir = tmp_path / "new_collection"
        collection_dir.mkdir()
        # .portolan does not exist

        result = seed_collection_metadata(
            collection_dir,
            source_type="wfs",
            source_url="https://example.com/wfs",
            layer_name="test",
        )

        assert result is True
        assert (collection_dir / ".portolan").is_dir()
        assert (collection_dir / ".portolan" / "metadata.yaml").exists()

    def test_smart_description_selection_prefers_title_over_technical_abstract(
        self, tmp_path: Path
    ) -> None:
        """Uses title when abstract is a technical name."""
        import yaml

        from portolan_cli.extract.common.metadata_seeding import seed_collection_metadata

        collection_dir = tmp_path / "smart_select"
        collection_dir.mkdir()

        seed_collection_metadata(
            collection_dir,
            source_type="wfs",
            source_url="https://example.com/wfs",
            layer_name="inspire_bu:BU.Building_building_emprise",
            title="Building - building_emprise",
            description="bu_building_emprise",  # technical underscore name
        )

        content = (collection_dir / ".portolan" / "metadata.yaml").read_text()
        metadata = yaml.safe_load(content)
        # Should use title (with spaces/dashes) not abstract (underscores)
        assert metadata["description"] == "Building - building_emprise"

    def test_smart_description_selection_uses_good_abstract(self, tmp_path: Path) -> None:
        """Uses abstract when it's a real description."""
        import yaml

        from portolan_cli.extract.common.metadata_seeding import seed_collection_metadata

        collection_dir = tmp_path / "good_abstract"
        collection_dir.mkdir()

        seed_collection_metadata(
            collection_dir,
            source_type="wfs",
            source_url="https://example.com/wfs",
            layer_name="buildings",
            title="Buildings Layer",
            description="Footprints of all buildings in the region with roof types",
        )

        content = (collection_dir / ".portolan" / "metadata.yaml").read_text()
        metadata = yaml.safe_load(content)
        # Should use the good abstract, not fall back to title
        assert (
            metadata["description"] == "Footprints of all buildings in the region with roof types"
        )
