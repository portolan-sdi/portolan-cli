"""Tests for portolan_cli.partitioning module."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from unittest import mock

import pytest

if TYPE_CHECKING:
    from datetime import datetime

    import pystac


def _world_extent() -> pystac.Extent:
    """Return a whole-world extent with an open interval."""
    import pystac

    interval: list[list[datetime | None]] = [[None, None]]
    return pystac.Extent(
        spatial=pystac.SpatialExtent(bboxes=[[-180.0, -90.0, 180.0, 90.0]]),
        temporal=pystac.TemporalExtent(intervals=interval),
    )


class TestShouldPartition:
    """Tests for should_partition function."""

    @pytest.mark.unit
    def test_should_partition_returns_false_for_small_file(self, tmp_path: Path) -> None:
        """Files under threshold should not be partitioned."""
        from portolan_cli.partitioning import should_partition

        # Create a small file (1 MB)
        small_file = tmp_path / "small.parquet"
        small_file.write_bytes(b"x" * (1 * 1024 * 1024))

        result = should_partition(small_file, threshold_gb=2.0)

        assert result is False

    @pytest.mark.unit
    def test_should_partition_returns_true_for_large_file(self, tmp_path: Path) -> None:
        """Files over threshold should be partitioned."""
        import os

        from portolan_cli.partitioning import should_partition

        large_file = tmp_path / "large.parquet"
        large_file.write_bytes(b"x" * 100)  # Small actual file

        original_stat = os.stat

        def mock_stat(path: str | Path, *args: Any, **kwargs: Any) -> os.stat_result:
            if Path(path) == large_file:
                # Return a mock stat result with 3GB size
                return os.stat_result((0, 0, 0, 0, 0, 0, 3 * 1024 * 1024 * 1024, 0, 0, 0))
            return original_stat(path, *args, **kwargs)

        with mock.patch("os.stat", mock_stat):
            result = should_partition(large_file, threshold_gb=2.0)

        assert result is True

    @pytest.mark.unit
    def test_should_partition_uses_config_threshold(self, tmp_path: Path) -> None:
        """should_partition respects custom threshold."""
        import os

        from portolan_cli.partitioning import should_partition

        file_path = tmp_path / "medium.parquet"
        file_path.write_bytes(b"x" * 100)

        original_stat = os.stat

        def mock_stat(path: str | Path, *args: Any, **kwargs: Any) -> os.stat_result:
            if Path(path) == file_path:
                # Return a mock stat result with 500MB size
                return os.stat_result((0, 0, 0, 0, 0, 0, 500 * 1024 * 1024, 0, 0, 0))
            return original_stat(path, *args, **kwargs)

        with mock.patch("os.stat", mock_stat):
            # With 2GB threshold: should NOT partition
            assert should_partition(file_path, threshold_gb=2.0) is False

            # With 0.4GB threshold: should partition
            assert should_partition(file_path, threshold_gb=0.4) is True

    @pytest.mark.unit
    def test_should_partition_returns_false_when_disabled(self, tmp_path: Path) -> None:
        """should_partition returns False when partitioning is disabled."""
        import os

        from portolan_cli.partitioning import should_partition

        large_file = tmp_path / "large.parquet"
        large_file.write_bytes(b"x" * 100)

        original_stat = os.stat

        def mock_stat(path: str | Path, *args: Any, **kwargs: Any) -> os.stat_result:
            if Path(path) == large_file:
                return os.stat_result((0, 0, 0, 0, 0, 0, 5 * 1024 * 1024 * 1024, 0, 0, 0))
            return original_stat(path, *args, **kwargs)

        with mock.patch("os.stat", mock_stat):
            result = should_partition(large_file, threshold_gb=2.0, enabled=False)

        assert result is False


class TestPartitionGeoparquet:
    """Tests for partition_geoparquet function."""

    @pytest.mark.unit
    def test_partition_geoparquet_calls_gpio_kdtree(self, tmp_path: Path) -> None:
        """partition_geoparquet should call geoparquet-io partition_by_kdtree."""
        from portolan_cli.partitioning import partition_geoparquet

        input_file = tmp_path / "input.parquet"
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        with mock.patch(
            "geoparquet_io.core.partition.by_kdtree.partition_by_kdtree"
        ) as mock_partition:
            partition_geoparquet(
                input_path=input_file,
                output_dir=output_dir,
                strategy="kdtree",
                target_rows=120_000,
            )

            mock_partition.assert_called_once()
            call_kwargs = mock_partition.call_args.kwargs
            assert call_kwargs["input_parquet"] == str(input_file)
            assert call_kwargs["output_folder"] == str(output_dir)
            assert call_kwargs["hive"] is True  # Per
            assert call_kwargs["auto_target_rows"] == ("rows", 120_000)

    @pytest.mark.unit
    def test_partition_geoparquet_returns_partition_files(self, tmp_path: Path) -> None:
        """partition_geoparquet should return list of created partition files."""
        from portolan_cli.partitioning import partition_geoparquet

        input_file = tmp_path / "input.parquet"
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Create mock partition output structure (Hive-style)
        (output_dir / "kdtree_cell=001").mkdir()
        (output_dir / "kdtree_cell=001" / "data.parquet").write_bytes(b"x")
        (output_dir / "kdtree_cell=002").mkdir()
        (output_dir / "kdtree_cell=002" / "data.parquet").write_bytes(b"x")

        with mock.patch("geoparquet_io.core.partition.by_kdtree.partition_by_kdtree"):
            result = partition_geoparquet(
                input_path=input_file,
                output_dir=output_dir,
                strategy="kdtree",
            )

        assert len(result) == 2
        assert all(p.name == "data.parquet" for p in result)
        assert {p.parent.name for p in result} == {"kdtree_cell=001", "kdtree_cell=002"}


class TestGetPartitionInfo:
    """Tests for get_partition_info function."""

    @pytest.mark.unit
    def test_get_partition_info_extracts_cell_id(self, tmp_path: Path) -> None:
        """get_partition_info should extract partition cell ID from path."""
        from portolan_cli.partitioning import get_partition_info

        partition_path = tmp_path / "kdtree_cell=042" / "data.parquet"
        partition_path.parent.mkdir(parents=True)
        partition_path.write_bytes(b"x")

        result = get_partition_info(partition_path)

        assert result["cell_id"] == "042"
        assert result["partition_column"] == "kdtree_cell"

    @pytest.mark.unit
    def test_get_partition_info_handles_different_strategies(self, tmp_path: Path) -> None:
        """get_partition_info should work with different partition column names."""
        from portolan_cli.partitioning import get_partition_info

        # H3 partition
        h3_path = tmp_path / "h3_cell=8928308280fffff" / "data.parquet"
        h3_path.parent.mkdir(parents=True)
        h3_path.write_bytes(b"x")

        result = get_partition_info(h3_path)

        assert result["cell_id"] == "8928308280fffff"
        assert result["partition_column"] == "h3_cell"


class TestGlobPatterns:
    """Tests for glob pattern building functions."""

    @pytest.mark.unit
    def test_build_glob_pattern_returns_relative_path_with_strategy(self) -> None:
        """build_glob_pattern returns relative glob with strategy-specific partition column."""
        from portolan_cli.partitioning import build_glob_pattern

        result = build_glob_pattern(strategy="kdtree")

        assert result == "./kdtree_cell=*/*.parquet"

    @pytest.mark.unit
    def test_build_glob_pattern_uses_correct_partition_column(self) -> None:
        """build_glob_pattern uses correct partition column for each strategy."""
        from portolan_cli.partitioning import build_glob_pattern

        assert build_glob_pattern("kdtree") == "./kdtree_cell=*/*.parquet"
        assert build_glob_pattern("h3") == "./h3_cell=*/*.parquet"
        assert build_glob_pattern("s2") == "./s2_cell=*/*.parquet"
        assert build_glob_pattern("quadkey") == "./quadkey=*/*.parquet"

    @pytest.mark.unit
    def test_build_remote_glob_creates_absolute_url(self) -> None:
        """build_remote_glob creates full remote URL with glob pattern."""
        from portolan_cli.partitioning import build_remote_glob

        result = build_remote_glob("s3://bucket/catalog", "buildings", "kdtree")

        assert result == "s3://bucket/catalog/buildings/kdtree_cell=*/*.parquet"

    @pytest.mark.unit
    def test_build_remote_glob_handles_trailing_slash(self) -> None:
        """build_remote_glob handles trailing slash in base URL."""
        from portolan_cli.partitioning import build_remote_glob

        result = build_remote_glob("s3://bucket/catalog/", "buildings", "kdtree")

        assert result == "s3://bucket/catalog/buildings/kdtree_cell=*/*.parquet"


class TestGlobTransformation:
    """Tests for partition:glob field transformation on push."""

    @pytest.mark.unit
    def test_transform_adds_glob_field_to_pattern_assets(self) -> None:
        """_transform_collection_glob_assets adds partition:glob to glob-pattern assets."""
        import json

        from portolan_cli.sync.push import _transform_collection_glob_assets

        collection_json = {
            "type": "Collection",
            "id": "buildings",
            "assets": {
                "partitioned_data": {
                    "href": "./kdtree_cell=*/*.parquet",
                    "type": "application/vnd.apache.parquet",
                    "roles": ["data"],
                },
                "thumbnail": {
                    "href": "./thumbnail.png",
                    "type": "image/png",
                    "roles": ["thumbnail"],
                },
            },
        }

        content = json.dumps(collection_json).encode("utf-8")
        result = _transform_collection_glob_assets(content, "s3://bucket/catalog", "buildings")
        result_json = json.loads(result)

        # Glob asset should have partition:glob added
        partitioned = result_json["assets"]["partitioned_data"]
        assert (
            partitioned["partition:glob"] == "s3://bucket/catalog/buildings/kdtree_cell=*/*.parquet"
        )

        # Non-glob asset should be unchanged
        thumbnail = result_json["assets"]["thumbnail"]
        assert "partition:glob" not in thumbnail

    @pytest.mark.unit
    def test_transform_overwrites_existing_glob_for_sync(self) -> None:
        """_transform_collection_glob_assets overwrites existing globs to keep in sync."""
        import json

        from portolan_cli.sync.push import _transform_collection_glob_assets

        collection_json = {
            "type": "Collection",
            "id": "buildings",
            "assets": {
                "partitioned_data": {
                    "href": "./kdtree_cell=*/*.parquet",
                    "type": "application/vnd.apache.parquet",
                    "partition:glob": "s3://old-bucket/old-path/*/*.parquet",
                },
            },
        }

        content = json.dumps(collection_json).encode("utf-8")
        result = _transform_collection_glob_assets(content, "s3://bucket/catalog", "buildings")
        result_json = json.loads(result)

        # Should overwrite with current remote URL to keep in sync
        assert (
            result_json["assets"]["partitioned_data"]["partition:glob"]
            == "s3://bucket/catalog/buildings/kdtree_cell=*/*.parquet"
        )

    @pytest.mark.unit
    def test_transform_returns_unchanged_for_no_globs(self) -> None:
        """_transform_collection_glob_assets returns unchanged content when no globs."""
        import json

        from portolan_cli.sync.push import _transform_collection_glob_assets

        collection_json = {
            "type": "Collection",
            "id": "buildings",
            "assets": {
                "data": {
                    "href": "./data.parquet",
                    "type": "application/vnd.apache.parquet",
                },
            },
        }

        content = json.dumps(collection_json).encode("utf-8")
        result = _transform_collection_glob_assets(content, "s3://bucket/catalog", "buildings")

        # Content should be unchanged — verify semantically since no globs present
        result_json = json.loads(result)
        assert result_json == collection_json

    @pytest.mark.unit
    def test_transform_handles_invalid_json(self) -> None:
        """_transform_collection_glob_assets returns unchanged for invalid JSON."""
        from portolan_cli.sync.push import _transform_collection_glob_assets

        content = b"not valid json {"
        result = _transform_collection_glob_assets(content, "s3://bucket/catalog", "buildings")

        assert result == content


class TestPartitioningRollback:
    """Tests for partition failure rollback (atomicity)."""

    @pytest.mark.unit
    def test_partition_failure_cleans_up_partial_directories(self, tmp_path: Path) -> None:
        """If partition_geoparquet fails, partial directories are removed."""
        from portolan_cli.partitioning import partition_geoparquet

        input_file = tmp_path / "input.parquet"
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Create partial partition directories (simulating mid-failure state)
        partial_dir = output_dir / "kdtree_cell=001"
        partial_dir.mkdir()
        (partial_dir / "data.parquet").write_bytes(b"partial")

        # Mock partition_by_kdtree to raise after partial output exists
        def failing_partition(*args: object, **kwargs: object) -> None:
            raise RuntimeError("Simulated partition failure")

        with (
            mock.patch(
                "geoparquet_io.core.partition.by_kdtree.partition_by_kdtree",
                failing_partition,
            ),
            pytest.raises(RuntimeError, match="Simulated partition failure"),
        ):
            partition_geoparquet(
                input_path=input_file,
                output_dir=output_dir,
                strategy="kdtree",
            )

        # Partial directories should be cleaned up
        assert not partial_dir.exists()


class TestCliStrategyValidation:
    """Tests for CLI strategy validation."""

    @pytest.mark.unit
    def test_partition_command_only_accepts_kdtree(self) -> None:
        """CLI should only accept kdtree strategy (others not implemented)."""
        from click.testing import CliRunner

        from portolan_cli.cli import partition

        runner = CliRunner()

        with runner.isolated_filesystem():
            Path("test.parquet").write_bytes(b"test")

            # Invalid strategy rejected by Click.Choice
            result = runner.invoke(partition, ["test.parquet", "output/", "--strategy", "h3"])

            assert result.exit_code != 0
            assert "Invalid value" in result.output or "invalid choice" in result.output.lower()


class TestPartitionGeoparquetUnsupportedStrategy:
    """Tests for partition_geoparquet ValueError on unsupported strategies."""

    @pytest.mark.unit
    def test_partition_geoparquet_raises_for_unsupported_strategy(self, tmp_path: Path) -> None:
        """partition_geoparquet raises ValueError for non-kdtree strategies."""
        from portolan_cli.partitioning import partition_geoparquet

        input_file = tmp_path / "input.parquet"
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        with pytest.raises(ValueError, match="not yet supported"):
            partition_geoparquet(
                input_path=input_file,
                output_dir=output_dir,
                strategy="h3",
            )


class TestGetPartitionMetadata:
    """Tests for get_partition_metadata function (Phase 3: STAC Partition Extension)."""

    @pytest.mark.unit
    def test_get_partition_metadata_returns_extension_fields(self, tmp_path: Path) -> None:
        """get_partition_metadata returns dict with partition:* fields."""
        from portolan_cli.partitioning import get_partition_metadata

        # Create Hive-style partition structure
        (tmp_path / "kdtree_cell=001").mkdir()
        (tmp_path / "kdtree_cell=001" / "data.parquet").write_bytes(b"x")
        (tmp_path / "kdtree_cell=002").mkdir()
        (tmp_path / "kdtree_cell=002" / "data.parquet").write_bytes(b"x")

        result = get_partition_metadata(tmp_path, strategy="kdtree")

        assert result["partition:scheme"] == "hive"
        assert result["partition:strategy"] == "kdtree"
        assert result["partition:file_count"] == 2
        keys = cast("list[dict[str, str]]", result["partition:keys"])
        assert len(keys) == 1
        assert keys[0]["name"] == "kdtree_cell"
        assert keys[0]["type"] == "string"

    @pytest.mark.unit
    def test_get_partition_metadata_counts_files_correctly(self, tmp_path: Path) -> None:
        """get_partition_metadata correctly counts files across partitions."""
        from portolan_cli.partitioning import get_partition_metadata

        # Create partitions with multiple files each
        (tmp_path / "kdtree_cell=001").mkdir()
        (tmp_path / "kdtree_cell=001" / "part1.parquet").write_bytes(b"x")
        (tmp_path / "kdtree_cell=001" / "part2.parquet").write_bytes(b"x")
        (tmp_path / "kdtree_cell=002").mkdir()
        (tmp_path / "kdtree_cell=002" / "part1.parquet").write_bytes(b"x")

        result = get_partition_metadata(tmp_path, strategy="kdtree")

        assert result["partition:file_count"] == 3

    @pytest.mark.unit
    def test_get_partition_metadata_uses_strategy_column(self, tmp_path: Path) -> None:
        """get_partition_metadata uses correct partition column per strategy."""
        from portolan_cli.partitioning import get_partition_metadata

        (tmp_path / "h3_cell=abc").mkdir()
        (tmp_path / "h3_cell=abc" / "data.parquet").write_bytes(b"x")

        result = get_partition_metadata(tmp_path, strategy="h3")

        keys = cast("list[dict[str, str]]", result["partition:keys"])
        assert keys[0]["name"] == "h3_cell"
        assert "H3" in keys[0]["description"]


class TestDetectPartitioning:
    """Tests for detect_partitioning function (Phase 4 prep)."""

    @pytest.mark.unit
    def test_detect_partitioning_finds_hive_structure(self, tmp_path: Path) -> None:
        """detect_partitioning detects Hive-style partitioning."""
        from portolan_cli.partitioning import detect_partitioning

        (tmp_path / "kdtree_cell=001").mkdir()
        (tmp_path / "kdtree_cell=001" / "data.parquet").write_bytes(b"x")
        (tmp_path / "kdtree_cell=002").mkdir()
        (tmp_path / "kdtree_cell=002" / "data.parquet").write_bytes(b"x")

        result = detect_partitioning(tmp_path)

        assert result is not None
        assert result["partition:scheme"] == "hive"
        assert result["partition:strategy"] == "kdtree"
        assert result["partition:file_count"] == 2

    @pytest.mark.unit
    def test_detect_partitioning_returns_none_for_flat_directory(self, tmp_path: Path) -> None:
        """detect_partitioning returns None for non-partitioned directories."""
        from portolan_cli.partitioning import detect_partitioning

        (tmp_path / "data.parquet").write_bytes(b"x")
        (tmp_path / "other.parquet").write_bytes(b"x")

        result = detect_partitioning(tmp_path)

        assert result is None

    @pytest.mark.unit
    def test_detect_partitioning_identifies_strategy_from_column_name(self, tmp_path: Path) -> None:
        """detect_partitioning identifies strategy from known column names."""
        from portolan_cli.partitioning import detect_partitioning

        (tmp_path / "h3_cell=8928308280fffff").mkdir()
        (tmp_path / "h3_cell=8928308280fffff" / "data.parquet").write_bytes(b"x")

        result = detect_partitioning(tmp_path)

        assert result is not None
        assert result["partition:strategy"] == "h3"

    @pytest.mark.unit
    def test_detect_partitioning_unknown_column_omits_strategy(self, tmp_path: Path) -> None:
        """detect_partitioning omits strategy key for unknown column names."""
        from portolan_cli.partitioning import detect_partitioning

        (tmp_path / "custom_partition=value1").mkdir()
        (tmp_path / "custom_partition=value1" / "data.parquet").write_bytes(b"x")

        result = detect_partitioning(tmp_path)

        assert result is not None
        # Strategy key should be omitted (not null) for unknown columns
        assert "partition:strategy" not in result
        assert result["partition:keys"][0]["name"] == "custom_partition"


class TestPartitionExtensionInStac:
    """Tests for partition extension integration in stac.py."""

    @pytest.mark.unit
    def test_add_partition_metadata_to_collection_adds_fields(self) -> None:
        """add_partition_metadata_to_collection adds partition:* fields."""
        import pystac

        from portolan_cli.stac import add_partition_metadata_to_collection

        collection = pystac.Collection(
            id="test",
            description="Test",
            extent=_world_extent(),
        )

        partition_metadata = {
            "partition:scheme": "hive",
            "partition:strategy": "kdtree",
            "partition:keys": [{"name": "kdtree_cell", "type": "string"}],
            "partition:file_count": 42,
        }

        add_partition_metadata_to_collection(collection, partition_metadata)

        assert collection.extra_fields["partition:scheme"] == "hive"
        assert collection.extra_fields["partition:strategy"] == "kdtree"
        assert collection.extra_fields["partition:file_count"] == 42
        assert len(collection.extra_fields["partition:keys"]) == 1

    @pytest.mark.unit
    def test_add_partition_metadata_registers_extension(self) -> None:
        """add_partition_metadata_to_collection adds extension URL."""
        import pystac

        from portolan_cli.stac import EXTENSION_URLS, add_partition_metadata_to_collection

        collection = pystac.Collection(
            id="test",
            description="Test",
            extent=_world_extent(),
        )

        partition_metadata: dict[str, object] = {"partition:scheme": "hive", "partition:keys": []}

        add_partition_metadata_to_collection(collection, partition_metadata)

        assert EXTENSION_URLS["partition"] in collection.stac_extensions

    @pytest.mark.unit
    def test_build_stac_extensions_includes_partition(self) -> None:
        """build_stac_extensions detects partition:* fields."""
        from portolan_cli.stac import EXTENSION_URLS, build_stac_extensions

        properties: dict[str, object] = {
            "partition:scheme": "hive",
            "partition:strategy": "kdtree",
            "partition:keys": [],
        }

        extensions = build_stac_extensions(properties)

        assert EXTENSION_URLS["partition"] in extensions


class TestFinalizeItemPartitionWiring:
    """Tests for partition metadata wiring through finalize_items."""

    @pytest.mark.unit
    def test_finalize_items_applies_partition_metadata_to_collection(self, tmp_path: Path) -> None:
        """finalize_items applies partition_metadata from PreparedItem to collection."""
        import pystac

        from portolan_cli.add import PreparedItem, finalize_items
        from portolan_cli.stac import EXTENSION_URLS

        # Initialize catalog structure
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()

        catalog = pystac.Catalog(id="test-catalog", description="Test")
        catalog.normalize_hrefs(f"{catalog_root}/")
        catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)

        # Create PreparedItem with partition_metadata
        partition_metadata = {
            "partition:scheme": "hive",
            "partition:strategy": "kdtree",
            "partition:keys": [{"name": "kdtree_cell", "type": "string"}],
            "partition:file_count": 42,
        }

        glob_asset = pystac.Asset(
            href="./kdtree_cell=*/*.parquet",
            media_type="application/vnd.apache.parquet",
            roles=["data"],
        )

        prepared = PreparedItem(
            item_id="test_partitioned",
            collection_id="test-collection",
            format_type="vector",
            bbox=[-180, -90, 180, 90],
            asset_files={},
            item_json_path=None,
            is_collection_level_asset=True,
            stac_assets={"test_partitioned": glob_asset},
            partition_metadata=partition_metadata,
        )

        # Finalize
        finalize_items(catalog_root, [prepared])

        # Verify collection has partition metadata
        collection_path = catalog_root / "test-collection" / "collection.json"
        assert collection_path.exists()

        collection = pystac.Collection.from_file(str(collection_path))

        assert collection.extra_fields.get("partition:scheme") == "hive"
        assert collection.extra_fields.get("partition:strategy") == "kdtree"
        assert collection.extra_fields.get("partition:file_count") == 42
        assert EXTENSION_URLS["partition"] in (collection.stac_extensions or [])


class TestGlobTransformationPartitionExtension:
    """Only partition:glob survives the transition."""

    @pytest.mark.unit
    def test_transform_writes_only_the_spec_field(self) -> None:
        """_transform_collection_glob_assets writes partition:glob and nothing else."""
        import json

        from portolan_cli.sync.push import _transform_collection_glob_assets

        collection_json = {
            "type": "Collection",
            "id": "buildings",
            "assets": {
                "partitioned_data": {
                    "href": "./kdtree_cell=*/*.parquet",
                    "type": "application/vnd.apache.parquet",
                },
            },
        }

        content = json.dumps(collection_json).encode("utf-8")
        result = _transform_collection_glob_assets(content, "s3://bucket/catalog", "buildings")
        result_json = json.loads(result)

        asset = result_json["assets"]["partitioned_data"]
        expected_glob = "s3://bucket/catalog/buildings/kdtree_cell=*/*.parquet"

        assert asset["partition:glob"] == expected_glob
        assert "portolan:glob" not in asset

    @pytest.mark.unit
    def test_transform_drops_the_legacy_field_it_finds(self) -> None:
        """A collection published by 0.8.0 loses portolan:glob on the next push."""
        import json

        from portolan_cli.sync.push import _transform_collection_glob_assets

        collection_json = {
            "type": "Collection",
            "id": "buildings",
            "assets": {
                "partitioned_data": {
                    "href": "./kdtree_cell=*/*.parquet",
                    "partition:glob": "s3://old-bucket/old-path/*/*.parquet",
                    "portolan:glob": "s3://old-bucket/old-path/*/*.parquet",
                },
            },
        }

        content = json.dumps(collection_json).encode("utf-8")
        result = _transform_collection_glob_assets(content, "s3://bucket/catalog", "buildings")
        result_json = json.loads(result)

        asset = result_json["assets"]["partitioned_data"]
        assert asset["partition:glob"] == "s3://bucket/catalog/buildings/kdtree_cell=*/*.parquet"
        assert "portolan:glob" not in asset


class TestGlobAssetKeyCollision:
    """Tests for glob asset insertion avoiding key collision."""

    @pytest.mark.unit
    def test_glob_asset_uses_alternate_key_when_target_occupied(self, tmp_path: Path) -> None:
        import pystac

        from portolan_cli.add import _ensure_partition_metadata

        # Create Hive partition structure
        collection_dir = tmp_path / "collection"
        (collection_dir / "year=2023").mkdir(parents=True)
        (collection_dir / "year=2023" / "data.parquet").write_bytes(b"x")

        # Create collection with existing non-glob asset at target key
        collection = pystac.Collection(
            id="test",
            description="Test collection",
            extent=_world_extent(),
        )
        # Add a non-glob asset at the target key "partitioned_data"
        collection.assets["partitioned_data"] = pystac.Asset(
            href="./existing_data.csv",  # Non-glob href
            media_type="text/csv",
            roles=["data"],
            title="User-defined asset",
        )

        # Run _ensure_partition_metadata with empty items (auto-detection path)
        _ensure_partition_metadata(collection, collection_dir, items=[])

        # Original asset should NOT be clobbered
        assert collection.assets["partitioned_data"].href == "./existing_data.csv"
        assert collection.assets["partitioned_data"].title == "User-defined asset"

        # Glob asset should be added at alternate key
        assert "partitioned_data_glob" in collection.assets
        assert "*" in collection.assets["partitioned_data_glob"].href

    @pytest.mark.unit
    def test_glob_asset_reuses_key_when_already_glob(self, tmp_path: Path) -> None:
        """When 'partitioned_data' key already has glob asset, skip adding."""
        import pystac

        from portolan_cli.add import _ensure_partition_metadata

        # Create Hive partition structure
        collection_dir = tmp_path / "collection"
        (collection_dir / "year=2023").mkdir(parents=True)
        (collection_dir / "year=2023" / "data.parquet").write_bytes(b"x")

        # Create collection with existing glob asset
        collection = pystac.Collection(
            id="test",
            description="Test collection",
            extent=_world_extent(),
        )
        collection.assets["existing_glob"] = pystac.Asset(
            href="./year=*/*.parquet",  # Already a glob
            media_type="application/vnd.apache.parquet",
            roles=["data"],
        )

        # Run _ensure_partition_metadata with empty items (auto-detection path)
        _ensure_partition_metadata(collection, collection_dir, items=[])

        # No new glob asset should be added (one already exists)
        assert "partitioned_data" not in collection.assets
        assert "partitioned_data_glob" not in collection.assets
        assert "existing_glob" in collection.assets


def _init_catalog(path: Path, config: str = "") -> None:
    """Create a minimal catalog with an optional config.yaml body."""
    portolan_dir = path / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text(config)
    (path / "catalog.json").write_text("{}")


class TestFindLargePartitionCandidates:
    """Tests for find_large_partition_candidates (config decision)."""

    @pytest.mark.unit
    def test_disabled_returns_no_candidates(self, tmp_path: Path) -> None:
        """partitioning.enabled=false short-circuits: no scan, empty candidates."""
        from portolan_cli.partitioning import find_large_partition_candidates

        _init_catalog(tmp_path, "partitioning:\n  enabled: false\n")
        parquet_file = tmp_path / "data.parquet"
        parquet_file.write_bytes(b"x" * 100)

        with mock.patch("portolan_cli.partitioning.should_partition", return_value=True):
            decision = find_large_partition_candidates([parquet_file], tmp_path)

        assert decision.enabled is False
        assert decision.large_files == []

    @pytest.mark.unit
    def test_prompt_disabled_returns_no_candidates(self, tmp_path: Path) -> None:
        """partitioning.prompt=false short-circuits even when enabled."""
        from portolan_cli.partitioning import find_large_partition_candidates

        _init_catalog(tmp_path, "partitioning:\n  enabled: true\n  prompt: false\n")
        parquet_file = tmp_path / "data.parquet"
        parquet_file.write_bytes(b"x" * 100)

        with mock.patch("portolan_cli.partitioning.should_partition", return_value=True):
            decision = find_large_partition_candidates([parquet_file], tmp_path)

        assert decision.prompt is False
        assert decision.large_files == []

    @pytest.mark.unit
    def test_flags_large_file(self, tmp_path: Path) -> None:
        """A file over threshold is returned as a candidate with its size."""
        from portolan_cli.partitioning import find_large_partition_candidates

        _init_catalog(tmp_path)
        parquet_file = tmp_path / "big.parquet"
        parquet_file.write_bytes(b"x" * 100)

        with mock.patch("portolan_cli.partitioning.should_partition", return_value=True):
            decision = find_large_partition_candidates([parquet_file], tmp_path)

        assert decision.enabled is True
        assert [p for p, _ in decision.large_files] == [parquet_file]

    @pytest.mark.unit
    def test_small_file_not_flagged(self, tmp_path: Path) -> None:
        """A small real file (well under 2GB) is not a candidate."""
        from portolan_cli.partitioning import find_large_partition_candidates

        _init_catalog(tmp_path)
        parquet_file = tmp_path / "small.parquet"
        parquet_file.write_bytes(b"x" * 100)

        decision = find_large_partition_candidates([parquet_file], tmp_path)

        assert decision.large_files == []

    @pytest.mark.unit
    def test_scans_directories_recursively(self, tmp_path: Path) -> None:
        """Directories are scanned recursively for parquet candidates."""
        from portolan_cli.partitioning import find_large_partition_candidates

        _init_catalog(tmp_path)
        nested = tmp_path / "data" / "nested"
        nested.mkdir(parents=True)
        parquet_file = nested / "deep.parquet"
        parquet_file.write_bytes(b"x" * 100)

        with mock.patch("portolan_cli.partitioning.should_partition", return_value=True):
            decision = find_large_partition_candidates([tmp_path / "data"], tmp_path)

        assert [p for p, _ in decision.large_files] == [parquet_file]

    @pytest.mark.unit
    def test_non_parquet_file_ignored(self, tmp_path: Path) -> None:
        """Non-parquet files are ignored even when should_partition is True."""
        from portolan_cli.partitioning import find_large_partition_candidates

        _init_catalog(tmp_path)
        other = tmp_path / "data.csv"
        other.write_bytes(b"x" * 100)

        with mock.patch("portolan_cli.partitioning.should_partition", return_value=True):
            decision = find_large_partition_candidates([other], tmp_path)

        assert decision.large_files == []


def _points_geoparquet(path: Path) -> Path:
    """Write a GeoParquet file of 1000 points to ``path``.

    geoparquet-io refuses partitions under 100 rows, so the 10-row fixture is too small.
    """
    import json

    import pyarrow as pa
    import pyarrow.parquet as pq
    import shapely

    geo = {
        "version": "1.0.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
    }
    points = [shapely.Point(i % 40, i // 40).wkb for i in range(1000)]
    table = pa.table({"id": list(range(1000)), "geometry": pa.array(points, pa.binary())})
    pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), path)
    return path


class TestPartitionFile:
    """Tests for partition_file, the library function of `portolan partition`."""

    @pytest.mark.integration
    def test_preview_envelope_is_the_library_result(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The preview envelope data equals partition_file(...).to_dict()."""
        import json

        from click.testing import CliRunner

        from portolan_cli.cli import cli
        from portolan_cli.partitioning import partition_file

        monkeypatch.chdir(tmp_path)
        Path("small.parquet").write_bytes(b"x" * 10)
        result = CliRunner().invoke(
            cli, ["--format", "json", "partition", "small.parquet", "--preview"]
        )

        assert result.exit_code == 0, result.output
        data = json.loads(result.stdout)["data"]
        assert data == partition_file(Path("small.parquet"), None, preview=True).to_dict()
        assert data == {
            "file": "small.parquet",
            "size_gb": 0.0,
            "recommended_partition": False,
            "strategy": "kdtree",
            "target_rows": 120_000,
        }

    @pytest.mark.integration
    def test_a_geoparquet_io_failure_is_a_partition_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A geoparquet-io failure raises PartitionError, and the envelope reports it."""
        import json

        from click.testing import CliRunner

        from portolan_cli.cli import cli
        from portolan_cli.partitioning import partition_file
        from portolan_core.errors import PartitionError

        def failing_partition(*args: object, **kwargs: object) -> None:
            raise RuntimeError("Simulated partition failure")

        monkeypatch.chdir(tmp_path)
        Path("input.parquet").write_bytes(b"x")
        with mock.patch(
            "geoparquet_io.core.partition.by_kdtree.partition_by_kdtree", failing_partition
        ):
            with pytest.raises(PartitionError) as caught:
                partition_file(Path("input.parquet"), Path("out"))
            result = CliRunner().invoke(
                cli, ["--format", "json", "partition", "input.parquet", "out2"]
            )
            text = CliRunner().invoke(cli, ["partition", "input.parquet", "out3"])

        assert caught.value.message == "Simulated partition failure"
        assert Path("out").is_dir()
        assert result.exit_code == 1
        # The envelope keeps the shape of the command before PartitionError: no code.
        assert json.loads(result.stdout)["errors"] == [
            {"type": "PartitionError", "message": "Simulated partition failure"}
        ]
        assert text.exit_code == 1
        assert "Partitioning failed: Simulated partition failure" in text.output

    @pytest.mark.integration
    def test_partition_envelope_is_the_library_result(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A partition writes the same files on both sides, and the envelope is the result."""
        import json

        from click.testing import CliRunner

        from portolan_cli.cli import cli
        from portolan_cli.partitioning import PartitionOutcome, partition_file

        monkeypatch.chdir(tmp_path)
        _points_geoparquet(Path("points.parquet"))
        result = CliRunner().invoke(
            cli, ["--format", "json", "partition", "points.parquet", "cli/out"]
        )
        outcome = partition_file(Path("points.parquet"), Path("lib/out"))

        assert result.exit_code == 0, result.output
        assert isinstance(outcome, PartitionOutcome)
        data = json.loads(result.stdout)["data"]

        def expected(output_dir: Path) -> dict[str, object]:
            # str(Path) gives backslashes on Windows.
            on_disk = sorted(str(f) for f in output_dir.rglob("*.parquet"))
            return {
                "input": "points.parquet",
                "output_dir": str(output_dir),
                "partitions": len(on_disk),
                "files": on_disk,
            }

        assert len(list(Path("lib/out").rglob("*.parquet"))) > 1
        assert outcome.to_dict() == expected(Path("lib/out"))
        assert data == expected(Path("cli/out"))

    @pytest.mark.integration
    def test_the_library_call_prints_nothing(self, tmp_path: Path) -> None:
        source = _points_geoparquet(tmp_path / "points.parquet")

        result = _python(_PARTITION_CALL, str(source), str(tmp_path / "out"))

        assert list((tmp_path / "out").rglob("*.parquet"))
        assert (result.stdout, result.stderr) == ("", "")

    @pytest.mark.integration
    def test_a_caller_that_configures_logging_gets_the_warnings(self, tmp_path: Path) -> None:
        """The NullHandler keeps propagation, so a root handler gets geoparquet-io warnings."""
        source = _points_geoparquet(tmp_path / "points.parquet")
        configure = (
            "import logging\nlogging.basicConfig(format='%(name)s %(levelname)s %(message)s')\n"
        )

        result = _python(configure + _PARTITION_CALL, str(source), str(tmp_path / "out"))

        assert result.stdout == ""
        assert result.stderr.splitlines() == [
            "geoparquet_io WARNING   Small partitions: Average of 500 rows per partition is"
            " below recommended 10,000. Suboptimal but might be intentional for your use case."
        ]

    @pytest.mark.integration
    def test_the_partition_command_output_is_unchanged(self, tmp_path: Path) -> None:
        """`portolan partition` shows its own lines and the geoparquet-io warning, not INFO."""
        import re

        _points_geoparquet(tmp_path / "points.parquet")

        result = _portolan(tmp_path, "partition", "points.parquet", "out")

        assert result.stdout == (
            "\u2192 Partitioning points.parquet with kdtree strategy...\n"
            "\u2713 Created 2 partitions in out\n"
        )
        assert [re.sub(r"^\+\d+\.\d+s ", "", line) for line in result.stderr.splitlines()] == [
            "WARNING geoparquet_io:   Small partitions: Average of 500 rows per partition is"
            " below recommended 10,000. Suboptimal but might be intentional for your use case."
        ]

    @pytest.mark.integration
    def test_the_add_command_output_is_unchanged(self, tmp_path: Path) -> None:
        """`portolan add` of a GeoParquet file that geoparquet-io rewrites prints only its own lines."""
        _portolan(tmp_path, "init", "--auto", "--license", "CC-BY-4.0")
        # The subprocess escapes the no_basemap_tiles fixture, so it draws no thumbnail.
        _portolan(tmp_path, "config", "set", "thumbnails.enabled", "false")
        (tmp_path / "points").mkdir()
        _points_geoparquet(tmp_path / "points" / "points.parquet")

        result = _portolan(tmp_path, "add", "points")

        assert result.stdout.splitlines()[0].startswith("\u2192 Rewriting points.parquet (")
        assert result.stdout.splitlines()[1:] == ["\u2713 Added 1 file to 1 collection"]
        assert result.stderr == ""


_PARTITION_CALL = (
    "import sys\n"
    "from pathlib import Path\n"
    "from portolan_cli.partitioning import partition_file\n"
    "partition_file(Path(sys.argv[1]), Path(sys.argv[2]))\n"
)


def _python(code: str, *args: str, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    """Run ``code`` in a fresh interpreter, so no handler of this test process applies."""
    # A Windows pipe defaults to cp1252, which cannot encode the output marks.
    env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    return subprocess.run(
        [sys.executable, "-c", code, *args],
        capture_output=True,
        encoding="utf-8",
        check=True,
        cwd=cwd,
        env=env,
    )


def _portolan(cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    """Run the ``portolan`` command in a fresh process in ``cwd``."""
    return _python("from portolan_cli import cli\ncli()\n", *args, cwd=cwd)
