"""Tests for README catalog-level aggregation.

Tests that catalog-level READMEs aggregate extent and collection info
from child collections.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.readme import aggregate_catalog_extent, generate_catalog_readme


class TestAggregateCatalogExtent:
    """Tests for aggregate_catalog_extent function."""

    @pytest.mark.unit
    def test_aggregates_bbox_envelope(self, tmp_path: Path) -> None:
        """Bbox should be the envelope of all collection bboxes."""
        # Create two collections with different extents
        (tmp_path / "collection-a").mkdir()
        (tmp_path / "collection-a" / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "collection-a",
                    "extent": {
                        "spatial": {"bbox": [[-10, -20, 10, 20]]},
                        "temporal": {
                            "interval": [["2020-01-01T00:00:00Z", "2020-12-31T00:00:00Z"]]
                        },
                    },
                }
            )
        )

        (tmp_path / "collection-b").mkdir()
        (tmp_path / "collection-b" / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "collection-b",
                    "extent": {
                        "spatial": {"bbox": [[5, 10, 30, 40]]},
                        "temporal": {
                            "interval": [["2021-06-01T00:00:00Z", "2021-12-31T00:00:00Z"]]
                        },
                    },
                }
            )
        )

        result = aggregate_catalog_extent(tmp_path)

        # Envelope: min of mins, max of maxes
        assert result["bbox"] == [-10, -20, 30, 40]

    @pytest.mark.unit
    def test_aggregates_temporal_extent(self, tmp_path: Path) -> None:
        """Temporal extent should span earliest to latest."""
        (tmp_path / "early").mkdir()
        (tmp_path / "early" / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "early",
                    "extent": {
                        "spatial": {"bbox": [[0, 0, 1, 1]]},
                        "temporal": {
                            "interval": [["2018-01-01T00:00:00Z", "2019-12-31T00:00:00Z"]]
                        },
                    },
                }
            )
        )

        (tmp_path / "late").mkdir()
        (tmp_path / "late" / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "late",
                    "extent": {
                        "spatial": {"bbox": [[0, 0, 1, 1]]},
                        "temporal": {
                            "interval": [["2022-01-01T00:00:00Z", "2023-12-31T00:00:00Z"]]
                        },
                    },
                }
            )
        )

        result = aggregate_catalog_extent(tmp_path)

        assert result["temporal_start"] == "2018-01-01T00:00:00Z"
        assert result["temporal_end"] == "2023-12-31T00:00:00Z"

    @pytest.mark.unit
    def test_lists_collection_ids(self, tmp_path: Path) -> None:
        """Should return list of collection IDs."""
        for name in ["alpha", "beta", "gamma"]:
            (tmp_path / name).mkdir()
            (tmp_path / name / "collection.json").write_text(
                json.dumps(
                    {
                        "type": "Collection",
                        "id": name,
                        "extent": {
                            "spatial": {"bbox": [[0, 0, 1, 1]]},
                            "temporal": {"interval": [[None, None]]},
                        },
                    }
                )
            )

        result = aggregate_catalog_extent(tmp_path)

        assert sorted(result["collections"]) == ["alpha", "beta", "gamma"]

    @pytest.mark.unit
    def test_handles_null_temporal_extent(self, tmp_path: Path) -> None:
        """Collections with null temporal extent should be handled gracefully."""
        (tmp_path / "provisional").mkdir()
        (tmp_path / "provisional" / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "provisional",
                    "extent": {
                        "spatial": {"bbox": [[0, 0, 1, 1]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                }
            )
        )

        result = aggregate_catalog_extent(tmp_path)

        # Should not crash, temporal can be None
        assert result["temporal_start"] is None
        assert result["temporal_end"] is None

    @pytest.mark.unit
    def test_returns_empty_for_no_collections(self, tmp_path: Path) -> None:
        """Empty catalog should return empty aggregation."""
        result = aggregate_catalog_extent(tmp_path)

        assert result["collections"] == []
        assert result["bbox"] is None

    @pytest.mark.unit
    def test_ignores_non_collection_directories(self, tmp_path: Path) -> None:
        """Directories without collection.json should be ignored."""
        (tmp_path / ".portolan").mkdir()  # Internal dir
        (tmp_path / "not-a-collection").mkdir()  # No collection.json

        (tmp_path / "real-collection").mkdir()
        (tmp_path / "real-collection" / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "real-collection",
                    "extent": {
                        "spatial": {"bbox": [[0, 0, 1, 1]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                }
            )
        )

        result = aggregate_catalog_extent(tmp_path)

        assert result["collections"] == ["real-collection"]


class TestGenerateCatalogReadme:
    """Tests for generate_catalog_readme function."""

    @pytest.mark.unit
    def test_includes_catalog_title(self, tmp_path: Path) -> None:
        """Catalog README should use catalog.json title."""
        (tmp_path / "catalog.json").write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "my-catalog",
                    "title": "My Data Catalog",
                    "description": "A collection of datasets",
                }
            )
        )

        readme = generate_catalog_readme(tmp_path)

        assert "# My Data Catalog" in readme

    @pytest.mark.unit
    def test_includes_collections_section(self, tmp_path: Path) -> None:
        """Catalog README should list collections."""
        (tmp_path / "catalog.json").write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "my-catalog",
                    "title": "My Catalog",
                    "description": "Test",
                }
            )
        )

        (tmp_path / "demographics").mkdir()
        (tmp_path / "demographics" / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "demographics",
                    "title": "Demographics Data",
                    "description": "Census and population data",
                    "extent": {
                        "spatial": {"bbox": [[-125, 24, -66, 50]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                }
            )
        )

        readme = generate_catalog_readme(tmp_path)

        assert "## Collections" in readme
        assert "demographics" in readme
        assert "Demographics Data" in readme or "Census" in readme

    @pytest.mark.unit
    def test_includes_aggregated_extent(self, tmp_path: Path) -> None:
        """Catalog README should show aggregated spatial extent."""
        (tmp_path / "catalog.json").write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "test",
                    "title": "Test",
                    "description": "Test",
                }
            )
        )

        (tmp_path / "west").mkdir()
        (tmp_path / "west" / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "west",
                    "extent": {
                        "spatial": {"bbox": [[-125, 30, -100, 50]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                }
            )
        )

        (tmp_path / "east").mkdir()
        (tmp_path / "east" / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "east",
                    "extent": {
                        "spatial": {"bbox": [[-80, 25, -65, 45]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                }
            )
        )

        readme = generate_catalog_readme(tmp_path)

        # Should show aggregated bbox covering both coasts
        assert "Spatial" in readme or "Coverage" in readme
        # The readme should reflect the full US extent
        assert "-125" in readme  # West bound
        assert "-65" in readme or "-66" in readme  # East bound


class TestCollapsibleCollections:
    """Tests for collapsible collections list (#424).

    Large catalogs (many collections) should use <details> tags to make
    the collections list collapsible, improving README navigability.
    """

    def _create_collections(self, tmp_path: Path, count: int) -> None:
        """Helper to create N collections."""
        (tmp_path / "catalog.json").write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "test-catalog",
                    "title": "Test Catalog",
                    "description": "Catalog for testing collapsible collections",
                }
            )
        )
        for i in range(count):
            coll_dir = tmp_path / f"collection-{i:03d}"
            coll_dir.mkdir()
            (coll_dir / "collection.json").write_text(
                json.dumps(
                    {
                        "type": "Collection",
                        "id": f"collection-{i:03d}",
                        "title": f"Collection {i}",
                        "description": f"Test collection number {i}",
                        "extent": {
                            "spatial": {"bbox": [[0, 0, 1, 1]]},
                            "temporal": {"interval": [[None, None]]},
                        },
                    }
                )
            )

    @pytest.mark.unit
    def test_small_catalog_not_collapsible(self, tmp_path: Path) -> None:
        """Catalogs with few collections should NOT use <details> tags."""
        self._create_collections(tmp_path, 5)

        readme = generate_catalog_readme(tmp_path)

        # Should NOT contain <details> tags for small catalogs
        assert "<details>" not in readme
        assert "</details>" not in readme
        # Should still list collections normally
        assert "## Collections" in readme
        assert "Collection 0" in readme

    @pytest.mark.unit
    def test_large_catalog_is_collapsible(self, tmp_path: Path) -> None:
        """Catalogs with many collections SHOULD use <details> tags."""
        self._create_collections(tmp_path, 15)

        readme = generate_catalog_readme(tmp_path)

        # Should contain <details> tags
        assert "<details>" in readme
        assert "</details>" in readme
        # Should have summary with count
        assert "<summary>" in readme
        assert "15 collections" in readme

    @pytest.mark.unit
    def test_collapsible_at_threshold(self, tmp_path: Path) -> None:
        """Collections should be collapsible at exactly the threshold."""
        # Default threshold is 10
        self._create_collections(tmp_path, 10)

        readme = generate_catalog_readme(tmp_path)

        # At threshold, should be collapsible
        assert "<details>" in readme
        assert "10 collections" in readme

    @pytest.mark.unit
    def test_just_below_threshold_not_collapsible(self, tmp_path: Path) -> None:
        """Collections just below threshold should NOT be collapsible."""
        self._create_collections(tmp_path, 9)

        readme = generate_catalog_readme(tmp_path)

        # Just below threshold, should NOT be collapsible
        assert "<details>" not in readme

    @pytest.mark.unit
    def test_collapsible_contains_all_collections(self, tmp_path: Path) -> None:
        """Collapsible section should contain all collection links."""
        self._create_collections(tmp_path, 12)

        readme = generate_catalog_readme(tmp_path)

        # All collections should be present
        for i in range(12):
            assert f"collection-{i:03d}" in readme
            assert f"Collection {i}" in readme

    @pytest.mark.unit
    def test_collapsible_summary_emoji(self, tmp_path: Path) -> None:
        """Summary should include folder emoji for visual clarity."""
        self._create_collections(tmp_path, 20)

        readme = generate_catalog_readme(tmp_path)

        # Summary should have emoji
        assert "📁" in readme

    @pytest.mark.unit
    def test_very_large_catalog_collapsible(self, tmp_path: Path) -> None:
        """Very large catalogs (100+ collections) should be collapsible."""
        self._create_collections(tmp_path, 100)

        readme = generate_catalog_readme(tmp_path)

        assert "<details>" in readme
        assert "100 collections" in readme
