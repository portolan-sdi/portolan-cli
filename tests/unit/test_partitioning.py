"""Tests for portolan_cli.partitioning module.

Tests the partitioning functionality including:
- Size threshold detection (should_partition)
- Partitioning via geoparquet-io wrapper
- Config integration
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from unittest import mock

import pytest

if TYPE_CHECKING:
    pass


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

        def mock_stat(path: str | Path, *args: object, **kwargs: object) -> os.stat_result:
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

        def mock_stat(path: str | Path, *args: object, **kwargs: object) -> os.stat_result:
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

        def mock_stat(path: str | Path, *args: object, **kwargs: object) -> os.stat_result:
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
            assert call_kwargs["hive"] is True  # Per ADR-0031
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
    """Tests for glob pattern building functions (Issue #351)."""

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
    """Tests for portolan:glob field transformation on push (Issue #351)."""

    @pytest.mark.unit
    def test_transform_adds_glob_field_to_pattern_assets(self) -> None:
        """_transform_collection_glob_assets adds portolan:glob to glob-pattern assets."""
        import json

        from portolan_cli.push import _transform_collection_glob_assets

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

        # Glob asset should have portolan:glob added
        partitioned = result_json["assets"]["partitioned_data"]
        assert "portolan:glob" in partitioned
        assert (
            partitioned["portolan:glob"] == "s3://bucket/catalog/buildings/kdtree_cell=*/*.parquet"
        )

        # Non-glob asset should be unchanged
        thumbnail = result_json["assets"]["thumbnail"]
        assert "portolan:glob" not in thumbnail

    @pytest.mark.unit
    def test_transform_skips_assets_with_existing_glob(self) -> None:
        """_transform_collection_glob_assets doesn't overwrite existing portolan:glob."""
        import json

        from portolan_cli.push import _transform_collection_glob_assets

        collection_json = {
            "type": "Collection",
            "id": "buildings",
            "assets": {
                "partitioned_data": {
                    "href": "./kdtree_cell=*/*.parquet",
                    "type": "application/vnd.apache.parquet",
                    "portolan:glob": "s3://existing/path/*/*.parquet",
                },
            },
        }

        content = json.dumps(collection_json).encode("utf-8")
        result = _transform_collection_glob_assets(content, "s3://bucket/catalog", "buildings")
        result_json = json.loads(result)

        # Should preserve existing value
        assert (
            result_json["assets"]["partitioned_data"]["portolan:glob"]
            == "s3://existing/path/*/*.parquet"
        )

    @pytest.mark.unit
    def test_transform_returns_unchanged_for_no_globs(self) -> None:
        """_transform_collection_glob_assets returns unchanged content when no globs."""
        import json

        from portolan_cli.push import _transform_collection_glob_assets

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
        from portolan_cli.push import _transform_collection_glob_assets

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

        with mock.patch(
            "geoparquet_io.core.partition.by_kdtree.partition_by_kdtree",
            failing_partition,
        ):
            with pytest.raises(RuntimeError, match="Simulated partition failure"):
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
        assert len(result["partition:keys"]) == 1
        assert result["partition:keys"][0]["name"] == "kdtree_cell"
        assert result["partition:keys"][0]["type"] == "string"

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

        assert result["partition:keys"][0]["name"] == "h3_cell"
        assert "H3" in result["partition:keys"][0]["description"]


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
    def test_detect_partitioning_unknown_column_returns_none_strategy(self, tmp_path: Path) -> None:
        """detect_partitioning returns None strategy for unknown column names."""
        from portolan_cli.partitioning import detect_partitioning

        (tmp_path / "custom_partition=value1").mkdir()
        (tmp_path / "custom_partition=value1" / "data.parquet").write_bytes(b"x")

        result = detect_partitioning(tmp_path)

        assert result is not None
        assert result["partition:strategy"] is None
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
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent(bboxes=[[-180, -90, 180, 90]]),
                temporal=pystac.TemporalExtent(intervals=[[None, None]]),
            ),
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
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent(bboxes=[[-180, -90, 180, 90]]),
                temporal=pystac.TemporalExtent(intervals=[[None, None]]),
            ),
        )

        partition_metadata = {"partition:scheme": "hive", "partition:keys": []}

        add_partition_metadata_to_collection(collection, partition_metadata)

        assert EXTENSION_URLS["partition"] in collection.stac_extensions

    @pytest.mark.unit
    def test_build_stac_extensions_includes_partition(self) -> None:
        """build_stac_extensions detects partition:* fields."""
        from portolan_cli.stac import EXTENSION_URLS, build_stac_extensions

        properties = {
            "partition:scheme": "hive",
            "partition:strategy": "kdtree",
            "partition:keys": [],
        }

        extensions = build_stac_extensions(properties)

        assert EXTENSION_URLS["partition"] in extensions


class TestFinalizeDatasetPartitionWiring:
    """Tests for partition metadata wiring through finalize_datasets (Issue #232 Phase 3)."""

    @pytest.mark.unit
    def test_finalize_datasets_applies_partition_metadata_to_collection(
        self, tmp_path: Path
    ) -> None:
        """finalize_datasets applies partition_metadata from PreparedDataset to collection."""
        import pystac

        from portolan_cli.dataset import PreparedDataset, finalize_datasets
        from portolan_cli.formats import FormatType
        from portolan_cli.stac import EXTENSION_URLS

        # Initialize catalog structure
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()

        catalog = pystac.Catalog(id="test-catalog", description="Test")
        catalog.normalize_hrefs(f"{catalog_root}/")
        catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)

        # Create PreparedDataset with partition_metadata
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

        prepared = PreparedDataset(
            item_id="test_partitioned",
            collection_id="test-collection",
            format_type=FormatType.VECTOR,
            bbox=[-180, -90, 180, 90],
            asset_files={},
            item_json_path=None,
            is_collection_level_asset=True,
            stac_assets={"test_partitioned": glob_asset},
            partition_metadata=partition_metadata,
        )

        # Finalize
        finalize_datasets(catalog_root, [prepared])

        # Verify collection has partition metadata
        collection_path = catalog_root / "test-collection" / "collection.json"
        assert collection_path.exists()

        collection = pystac.Collection.from_file(str(collection_path))

        assert collection.extra_fields.get("partition:scheme") == "hive"
        assert collection.extra_fields.get("partition:strategy") == "kdtree"
        assert collection.extra_fields.get("partition:file_count") == 42
        assert EXTENSION_URLS["partition"] in (collection.stac_extensions or [])


class TestGlobTransformationPartitionExtension:
    """Tests for partition:glob field emission (ADR-0042 transition)."""

    @pytest.mark.unit
    def test_transform_adds_both_glob_fields(self) -> None:
        """_transform_collection_glob_assets adds both partition:glob and portolan:glob."""
        import json

        from portolan_cli.push import _transform_collection_glob_assets

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

        # Both fields should be populated
        assert asset["partition:glob"] == expected_glob
        assert asset["portolan:glob"] == expected_glob

    @pytest.mark.unit
    def test_transform_respects_existing_partition_glob(self) -> None:
        """_transform_collection_glob_assets doesn't overwrite existing partition:glob."""
        import json

        from portolan_cli.push import _transform_collection_glob_assets

        collection_json = {
            "type": "Collection",
            "id": "buildings",
            "assets": {
                "partitioned_data": {
                    "href": "./kdtree_cell=*/*.parquet",
                    "partition:glob": "s3://existing/path/*/*.parquet",
                },
            },
        }

        content = json.dumps(collection_json).encode("utf-8")
        result = _transform_collection_glob_assets(content, "s3://bucket/catalog", "buildings")
        result_json = json.loads(result)

        # partition:glob preserved, portolan:glob added
        asset = result_json["assets"]["partitioned_data"]
        assert asset["partition:glob"] == "s3://existing/path/*/*.parquet"
        assert asset["portolan:glob"] == "s3://bucket/catalog/buildings/kdtree_cell=*/*.parquet"
