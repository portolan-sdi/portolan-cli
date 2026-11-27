"""Tests for README generation."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path


class TestGenerateReadme:
    """Tests for generate_readme function."""

    @pytest.mark.unit
    def test_generates_markdown_string(self) -> None:
        """generate_readme returns a markdown string."""
        from portolan_cli.readme import generate_readme

        stac = {
            "type": "Collection",
            "id": "demographics",
            "title": "Demographics Collection",
            "description": "Census demographics data",
        }
        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "CC-BY-4.0",
        }

        readme = generate_readme(stac=stac, metadata=metadata)

        assert isinstance(readme, str)
        assert len(readme) > 0
        assert "#" in readme  # Markdown

    @pytest.mark.unit
    def test_title_comes_from_stac(self) -> None:
        """generate_readme uses STAC title when metadata.yaml has no override."""
        from portolan_cli.readme import generate_readme

        stac = {"type": "Collection", "id": "test", "title": "STAC Title"}
        metadata = {
            "contact": {"name": "Name", "email": "a@b.c"},
            "license": "MIT",
        }

        readme = generate_readme(stac=stac, metadata=metadata)

        assert "# STAC Title" in readme

    @pytest.mark.unit
    def test_title_from_metadata_overrides_stac(self) -> None:
        from portolan_cli.readme import generate_readme

        stac = {
            "type": "Collection",
            "id": "depth-rp100",
            "title": "Depth Rp100",
            "description": "Depth Rp100",
        }
        metadata = {
            "contact": {"name": "Name", "email": "a@b.c"},
            "license": "MIT",
            "title": "Flood Depth - 100-Year Return Period",
            "description": "Modeled riverine flood water depth (meters).",
        }

        readme = generate_readme(stac=stac, metadata=metadata)

        assert "# Flood Depth - 100-Year Return Period" in readme
        assert "Modeled riverine flood water depth (meters)." in readme
        assert "# Depth Rp100" not in readme

    @pytest.mark.unit
    def test_blank_metadata_title_falls_back_to_stac(self) -> None:
        """A blank metadata.yaml title is treated as absent; STAC wins."""
        from portolan_cli.readme import generate_readme

        stac = {
            "type": "Collection",
            "id": "test",
            "title": "STAC Title",
            "description": "STAC description.",
        }
        metadata = {
            "contact": {"name": "Name", "email": "a@b.c"},
            "license": "MIT",
            "title": "   ",
            "description": "",
        }

        readme = generate_readme(stac=stac, metadata=metadata)

        assert "# STAC Title" in readme
        assert "STAC description." in readme

    @pytest.mark.unit
    def test_title_falls_back_to_id(self) -> None:
        """generate_readme uses STAC id if title not present."""
        from portolan_cli.readme import generate_readme

        stac = {"type": "Collection", "id": "my-collection"}
        metadata = {
            "contact": {"name": "Name", "email": "a@b.c"},
            "license": "MIT",
        }

        readme = generate_readme(stac=stac, metadata=metadata)

        assert "# my-collection" in readme

    @pytest.mark.unit
    def test_explicit_null_stac_fields_do_not_render_none(self) -> None:
        """Explicit nulls in STAC fall back, never render the string 'None'."""
        from portolan_cli.readme import generate_readme

        stac = {"type": "Collection", "id": None, "title": None, "description": None}
        metadata = {
            "contact": {"name": "Name", "email": "a@b.c"},
            "license": "MIT",
        }

        readme = generate_readme(stac=stac, metadata=metadata)

        assert "# Untitled Collection" in readme
        assert "None" not in readme

    @pytest.mark.unit
    def test_description_comes_from_stac(self) -> None:
        """generate_readme uses description from STAC."""
        from portolan_cli.readme import generate_readme

        stac = {
            "type": "Collection",
            "id": "test",
            "description": "This is the STAC description.",
        }
        metadata = {
            "contact": {"name": "Name", "email": "a@b.c"},
            "license": "MIT",
        }

        readme = generate_readme(stac=stac, metadata=metadata)

        assert "This is the STAC description" in readme

    @pytest.mark.unit
    def test_includes_spatial_coverage_from_stac(self) -> None:
        """generate_readme includes bounding box from STAC."""
        from portolan_cli.readme import generate_readme

        stac = {
            "type": "Collection",
            "id": "test",
            "extent": {
                "spatial": {"bbox": [[-122.5, 37.5, -121.5, 38.5]]},
            },
        }
        metadata = {
            "contact": {"name": "Name", "email": "a@b.c"},
            "license": "MIT",
        }

        readme = generate_readme(stac=stac, metadata=metadata)

        assert "-122.5" in readme
        assert "37.5" in readme

    @pytest.mark.unit
    def test_includes_columns_from_table_extension(self) -> None:
        """generate_readme includes columns from table:columns extension."""
        from portolan_cli.readme import generate_readme

        stac = {
            "type": "Collection",
            "id": "test",
            "summaries": {
                "table:columns": [
                    {"name": "geoid", "type": "string"},
                    {"name": "total_pop", "type": "int64"},
                    {"name": "geometry", "type": "geometry"},
                ]
            },
        }
        metadata = {
            "contact": {"name": "Name", "email": "a@b.c"},
            "license": "MIT",
        }

        readme = generate_readme(stac=stac, metadata=metadata)

        assert "geoid" in readme
        assert "total_pop" in readme
        assert "Schema" in readme or "Columns" in readme

    @pytest.mark.unit
    def test_includes_code_example_for_geoparquet(self) -> None:
        """generate_readme includes geopandas code example for GeoParquet."""
        from portolan_cli.readme import generate_readme

        stac = {
            "type": "Collection",
            "id": "test",
            "assets": {
                "data": {
                    "href": "data.parquet",
                    "type": "application/vnd.apache.parquet",
                }
            },
        }
        metadata = {
            "contact": {"name": "Name", "email": "a@b.c"},
            "license": "MIT",
        }

        readme = generate_readme(stac=stac, metadata=metadata)

        assert "geopandas" in readme.lower() or "read_parquet" in readme

    @pytest.mark.unit
    def test_includes_code_example_for_cog(self) -> None:
        """generate_readme includes rasterio code example for COG."""
        from portolan_cli.readme import generate_readme

        stac = {
            "type": "Collection",
            "id": "test",
            "assets": {
                "image": {
                    "href": "image.tif",
                    "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                }
            },
        }
        metadata = {
            "contact": {"name": "Name", "email": "a@b.c"},
            "license": "MIT",
        }

        readme = generate_readme(stac=stac, metadata=metadata)

        assert "rasterio" in readme.lower() or "open(" in readme

    @pytest.mark.unit
    def test_includes_checksums_from_assets(self) -> None:
        """generate_readme includes file checksums from STAC assets."""
        from portolan_cli.readme import generate_readme

        stac = {
            "type": "Collection",
            "id": "test",
            "assets": {
                "data": {
                    "href": "data.parquet",
                    "file:checksum": "sha256:abc123def456",
                    "file:size": 1048576,
                }
            },
        }
        metadata = {
            "contact": {"name": "Name", "email": "a@b.c"},
            "license": "MIT",
        }

        readme = generate_readme(stac=stac, metadata=metadata)

        # Should include some form of checksum reference
        assert "abc123" in readme or "checksum" in readme.lower() or "sha256" in readme.lower()

    @pytest.mark.unit
    def test_includes_stac_links(self) -> None:
        """generate_readme includes STAC catalog links."""
        from portolan_cli.readme import generate_readme

        stac = {
            "type": "Collection",
            "id": "test",
            "links": [
                {"rel": "self", "href": "collection.json"},
                {"rel": "root", "href": "../catalog.json"},
            ],
        }
        metadata = {
            "contact": {"name": "Name", "email": "a@b.c"},
            "license": "MIT",
        }

        readme = generate_readme(stac=stac, metadata=metadata)

        assert "STAC" in readme or "collection.json" in readme

    @pytest.mark.unit
    def test_includes_license_from_metadata(self) -> None:
        """generate_readme includes license from metadata.yaml."""
        from portolan_cli.readme import generate_readme

        stac = {"type": "Collection", "id": "test"}
        metadata = {
            "contact": {"name": "Name", "email": "a@b.c"},
            "license": "CC-BY-4.0",
            "license_url": "https://creativecommons.org/licenses/by/4.0/",
        }

        readme = generate_readme(stac=stac, metadata=metadata)

        assert "CC-BY-4.0" in readme

    @pytest.mark.unit
    def test_includes_contact_from_metadata(self) -> None:
        """generate_readme includes contact from metadata.yaml."""
        from portolan_cli.readme import generate_readme

        stac = {"type": "Collection", "id": "test"}
        metadata = {
            "contact": {"name": "Data Team", "email": "data@example.org"},
            "license": "MIT",
        }

        readme = generate_readme(stac=stac, metadata=metadata)

        assert "Data Team" in readme
        assert "data@example.org" in readme

    @pytest.mark.unit
    def test_includes_known_issues_when_present(self) -> None:
        """generate_readme includes known_issues from metadata.yaml."""
        from portolan_cli.readme import generate_readme

        stac = {"type": "Collection", "id": "test"}
        metadata = {
            "contact": {"name": "Name", "email": "a@b.c"},
            "license": "MIT",
            "known_issues": "Coverage gaps in rural areas for 2020 data.",
        }

        readme = generate_readme(stac=stac, metadata=metadata)

        assert "Known Issues" in readme or "known issues" in readme.lower()
        assert "Coverage gaps" in readme

    @pytest.mark.unit
    def test_includes_portolan_attribution(self) -> None:
        """generate_readme includes Portolan attribution footer."""
        from portolan_cli.readme import generate_readme

        stac = {"type": "Collection", "id": "test"}
        metadata = {
            "contact": {"name": "Name", "email": "a@b.c"},
            "license": "MIT",
        }

        readme = generate_readme(stac=stac, metadata=metadata)

        assert "Portolan" in readme


class TestCheckReadmeFreshness:
    """Tests for check_readme_freshness function."""

    @pytest.mark.unit
    def test_returns_true_when_readme_matches(self, tmp_path: Path) -> None:
        """check_readme_freshness returns True when README matches generated."""
        from portolan_cli.readme import check_readme_freshness, generate_readme

        stac = {"type": "Collection", "id": "test", "title": "Test"}
        metadata = {
            "contact": {"name": "Name", "email": "a@b.c"},
            "license": "MIT",
        }

        expected = generate_readme(stac=stac, metadata=metadata)
        readme_path = tmp_path / "README.md"
        readme_path.write_text(expected)

        is_fresh = check_readme_freshness(
            readme_path=readme_path,
            stac=stac,
            metadata=metadata,
        )

        assert is_fresh is True

    @pytest.mark.unit
    def test_returns_false_when_readme_differs(self, tmp_path: Path) -> None:
        """check_readme_freshness returns False when README differs from generated."""
        from portolan_cli.readme import check_readme_freshness

        stac = {"type": "Collection", "id": "test", "title": "Test"}
        metadata = {
            "contact": {"name": "Name", "email": "a@b.c"},
            "license": "MIT",
        }

        readme_path = tmp_path / "README.md"
        readme_path.write_text("# Different Content\n\nThis is stale.")

        is_fresh = check_readme_freshness(
            readme_path=readme_path,
            stac=stac,
            metadata=metadata,
        )

        assert is_fresh is False

    @pytest.mark.unit
    def test_returns_false_when_readme_missing(self, tmp_path: Path) -> None:
        """check_readme_freshness returns False when README doesn't exist."""
        from portolan_cli.readme import check_readme_freshness

        stac = {"type": "Collection", "id": "test"}
        metadata = {
            "contact": {"name": "Name", "email": "a@b.c"},
            "license": "MIT",
        }

        readme_path = tmp_path / "README.md"  # Doesn't exist

        is_fresh = check_readme_freshness(
            readme_path=readme_path,
            stac=stac,
            metadata=metadata,
        )

        assert is_fresh is False


class TestGenerateReadmeForCollection:
    """Tests for generate_readme_for_collection (high-level function)."""

    @pytest.mark.unit
    def test_loads_stac_and_metadata_from_collection(self, tmp_path: Path) -> None:
        """generate_readme_for_collection loads STAC and metadata from disk."""
        from portolan_cli.readme import generate_readme_for_collection

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()

        collection_dir = catalog_root / "demographics"
        collection_dir.mkdir()
        collection_json = {
            "type": "Collection",
            "id": "demographics",
            "title": "Demographics Collection",
            "description": "Census data",
            "extent": {"spatial": {"bbox": [[-125, 24, -66, 50]]}},
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json))

        (collection_dir / ".portolan").mkdir()
        (collection_dir / ".portolan" / "metadata.yaml").write_text(
            "contact:\n  name: Census Bureau\n  email: data@census.gov\nlicense: CC0-1.0\n"
        )

        readme = generate_readme_for_collection(
            collection_path=collection_dir,
            catalog_root=catalog_root,
        )

        assert "# Demographics Collection" in readme  # Title from STAC
        assert "-125" in readme  # bbox from STAC
        assert "Census Bureau" in readme  # contact from metadata

    @pytest.mark.unit
    def test_uses_hierarchical_metadata(self, tmp_path: Path) -> None:
        """generate_readme_for_collection uses merged metadata from hierarchy."""
        from portolan_cli.readme import generate_readme_for_collection

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "metadata.yaml").write_text(
            "contact:\n  name: Default Contact\n  email: default@example.org\nlicense: CC-BY-4.0\n"
        )

        collection_dir = catalog_root / "demographics"
        collection_dir.mkdir()
        (collection_dir / "collection.json").write_text(
            json.dumps({"type": "Collection", "id": "demographics", "title": "Demographics"})
        )
        (collection_dir / ".portolan").mkdir()
        (collection_dir / ".portolan" / "metadata.yaml").write_text(
            "known_issues: Some known issue\n"
        )

        readme = generate_readme_for_collection(
            collection_path=collection_dir,
            catalog_root=catalog_root,
        )

        assert "Default Contact" in readme  # Inherited from root
        assert "CC-BY-4.0" in readme  # Inherited from root
        assert "Some known issue" in readme  # From collection

    @pytest.mark.unit
    def test_handles_missing_collection_json(self, tmp_path: Path) -> None:
        """generate_readme_for_collection works when collection.json is missing."""
        from portolan_cli.readme import generate_readme_for_collection

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()

        collection_dir = catalog_root / "demographics"
        collection_dir.mkdir()
        (collection_dir / ".portolan").mkdir()
        (collection_dir / ".portolan" / "metadata.yaml").write_text(
            "contact:\n  name: Name\n  email: a@b.c\nlicense: MIT\n"
        )

        readme = generate_readme_for_collection(
            collection_path=collection_dir,
            catalog_root=catalog_root,
        )

        # Should work without STAC
        assert "Portolan" in readme  # At minimum, has footer


class TestGenerateReadmesWalkOrder:
    """generate_readmes settles the start directory before it lists the subtree."""

    @pytest.mark.unit
    def test_root_readme_is_written_before_the_subtree_listing_fails(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An OSError from the listing leaves the root README regenerated and reported."""
        from pathlib import Path as RealPath

        from portolan_cli.readme import generate_catalog_readme, generate_readmes

        (tmp_path / "catalog.json").write_text(
            json.dumps({"type": "Catalog", "id": "root", "description": "Root", "links": []})
        )
        (tmp_path / "README.md").write_text("stale\n")

        def fail_listing(self: Path, pattern: str) -> None:
            raise OSError("listing failed")

        messages: list[str] = []
        monkeypatch.setattr(RealPath, "rglob", fail_listing)
        with pytest.raises(OSError, match="listing failed"):
            generate_readmes(tmp_path, on_progress=messages.append)

        assert (tmp_path / "README.md").read_text() == generate_catalog_readme(tmp_path)
        assert messages == ["Processing catalog root", "Reading catalog.json from catalog root"]
