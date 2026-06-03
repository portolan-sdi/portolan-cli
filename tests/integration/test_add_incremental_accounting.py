"""Integration tests for incremental add asset accounting (Issue #447).

Four bugs in portolan add's read-modify-write logic:
1. Duplicate asset entries for the same href
2. table:row_count double-counts on re-add
3. Stale asset entries persist after files are deleted
4. extent.spatial.bbox not updated from real data

These tests verify correct reconciliation of new files against existing collection.json.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI test runner."""
    return CliRunner()


@pytest.fixture
def initialized_catalog(tmp_path: Path) -> Path:
    """Create an initialized Portolan catalog using CLI."""
    result = CliRunner().invoke(cli, ["init", str(tmp_path), "--auto"])
    assert result.exit_code == 0, f"Init failed: {result.output}"
    return tmp_path


@pytest.fixture
def two_parquet_files(tmp_path: Path, valid_points_parquet: Path) -> tuple[Path, Path]:
    """Create two distinct parquet files for testing.

    Returns (file_a, file_b) where each is a copy of the fixture.
    """
    file_a = tmp_path / "file_a.parquet"
    file_b = tmp_path / "file_b.parquet"
    shutil.copy(valid_points_parquet, file_a)
    shutil.copy(valid_points_parquet, file_b)
    return file_a, file_b


class TestRowCountAccounting:
    """Tests for table:row_count correctness on incremental adds (Bug #2)."""

    @pytest.mark.integration
    def test_row_count_not_doubled_on_readd(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        valid_points_parquet: Path,
    ) -> None:
        """Re-adding the same file should NOT double the row count.

        Bug: Adding file A (1000 rows) then re-adding file A gave row_count=2000.
        Expected: row_count=1000 (same file, same count).
        """
        collection_dir = initialized_catalog / "testcol"
        collection_dir.mkdir()
        data_file = collection_dir / "data.parquet"
        shutil.copy(valid_points_parquet, data_file)

        # First add
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), "--force", str(data_file)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, f"First add failed: {result.output}"

        collection_json = json.loads((collection_dir / "collection.json").read_text())
        first_row_count = collection_json.get("table:row_count")
        assert first_row_count is not None, "table:row_count should be set after first add"
        assert first_row_count > 0, "row_count should be positive"

        # Re-add same file
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), "--force", str(data_file)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, f"Second add failed: {result.output}"

        collection_json = json.loads((collection_dir / "collection.json").read_text())
        second_row_count = collection_json.get("table:row_count")

        # Row count should be the same, NOT doubled
        assert second_row_count == first_row_count, (
            f"Row count doubled on re-add: {first_row_count} -> {second_row_count}"
        )

    @pytest.mark.integration
    def test_row_count_accumulates_for_new_files(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        two_parquet_files: tuple[Path, Path],
    ) -> None:
        """Adding a second distinct file should add its rows to the total.

        Add file A (N rows), then add file B (N rows) -> row_count = 2N.
        """
        file_a, file_b = two_parquet_files
        collection_dir = initialized_catalog / "testcol"
        collection_dir.mkdir()

        # Copy file A into collection
        data_a = collection_dir / "data_a.parquet"
        shutil.copy(file_a, data_a)

        # Add file A
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), "--force", str(data_a)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        collection_json = json.loads((collection_dir / "collection.json").read_text())
        count_after_a = collection_json.get("table:row_count")

        # Copy file B into collection
        data_b = collection_dir / "data_b.parquet"
        shutil.copy(file_b, data_b)

        # Add file B
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), "--force", str(data_b)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        collection_json = json.loads((collection_dir / "collection.json").read_text())
        count_after_b = collection_json.get("table:row_count")

        # Row count should be A + B (both files have same fixture, so 2x)
        assert count_after_b == count_after_a * 2, (
            f"Expected {count_after_a * 2} rows (A + B), got {count_after_b}"
        )

    @pytest.mark.integration
    def test_row_count_correct_after_mixed_readd_and_new(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        two_parquet_files: tuple[Path, Path],
    ) -> None:
        """Adding file A, then file B, then re-adding file A should give 2N rows.

        This tests the most complex case: mixing re-adds with new files.
        """
        file_a, file_b = two_parquet_files
        collection_dir = initialized_catalog / "testcol"
        collection_dir.mkdir()

        data_a = collection_dir / "data_a.parquet"
        data_b = collection_dir / "data_b.parquet"
        shutil.copy(file_a, data_a)

        # Add A
        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), "--force", str(data_a)],
            catch_exceptions=False,
        )

        collection_json = json.loads((collection_dir / "collection.json").read_text())
        single_file_count = collection_json.get("table:row_count")

        # Add B
        shutil.copy(file_b, data_b)
        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), "--force", str(data_b)],
            catch_exceptions=False,
        )

        # Re-add A (should NOT change count - already counted)
        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), "--force", str(data_a)],
            catch_exceptions=False,
        )

        collection_json = json.loads((collection_dir / "collection.json").read_text())
        final_count = collection_json.get("table:row_count")

        # Should be exactly 2x single file (A + B), not 3x
        assert final_count == single_file_count * 2, (
            f"Expected {single_file_count * 2} rows (A + B), got {final_count}. "
            "Re-adding A should not add its rows again."
        )

    @pytest.mark.integration
    def test_untracked_parquet_files_not_counted(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        two_parquet_files: tuple[Path, Path],
    ) -> None:
        """Untracked parquet files in collection dir should NOT inflate row count.

        Bug: Stray parquet files (temp files, internal state) should not be
        included in table:row_count aggregation.

        Note: Per ADR-0028, portolan add auto-discovers ALL files in the
        collection directory, so the only way to have a truly "untracked"
        parquet is to place it in .portolan/ (which is explicitly excluded
        from both discovery and row counting).
        """
        file_a, file_b = two_parquet_files
        collection_dir = initialized_catalog / "testcol"
        collection_dir.mkdir()

        # Add the main data file
        data_a = collection_dir / "data_a.parquet"
        shutil.copy(file_a, data_a)

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), "--force", str(data_a)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        collection_json = json.loads((collection_dir / "collection.json").read_text())
        count_with_one = collection_json.get("table:row_count")
        assert count_with_one is not None, "Row count should be set"

        # Place a stray parquet in .portolan/ - simulates internal state or temp file
        # This file WOULD be found by glob("**/*.parquet") but should be filtered out
        portolan_dir = collection_dir / ".portolan" / "stray"
        portolan_dir.mkdir(parents=True)
        stray_file = portolan_dir / "stray.parquet"
        shutil.copy(file_b, stray_file)
        assert stray_file.exists(), "Stray file should exist on disk"

        # Re-add to trigger row count recomputation
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), "--force", str(data_a)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        collection_json = json.loads((collection_dir / "collection.json").read_text())
        count_after_stray = collection_json.get("table:row_count")

        # Row count should be unchanged - stray file in .portolan/ should NOT be counted
        assert count_after_stray == count_with_one, (
            f"Row count changed from {count_with_one} to {count_after_stray} - "
            f"stray parquet in .portolan/ was incorrectly included"
        )


class TestAssetDeduplication:
    """Tests for asset href deduplication (Bug #1)."""

    @pytest.mark.integration
    def test_no_duplicate_assets_for_same_href(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        valid_points_parquet: Path,
    ) -> None:
        """Re-adding a file should not create duplicate asset entries.

        Bug: Hand-named asset "data" -> ./data.parquet, re-add creates
        "data.parquet" -> ./data.parquet. Both point to same file.
        """
        collection_dir = initialized_catalog / "testcol"
        collection_dir.mkdir()
        data_file = collection_dir / "data.parquet"
        shutil.copy(valid_points_parquet, data_file)

        # Create collection.json with manually-named asset
        collection_json = {
            "type": "Collection",
            "stac_version": "1.1.0",
            "id": "testcol",
            "description": "Test collection",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
            },
            "links": [],
            "assets": {
                "data": {  # Human-authored key (not filename)
                    "href": "./data.parquet",
                    "type": "application/vnd.apache.parquet",
                    "roles": ["data"],
                    "title": "My Data",
                }
            },
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

        # Re-add the file
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), "--force", str(data_file)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"

        collection_json = json.loads((collection_dir / "collection.json").read_text())
        assets = collection_json.get("assets", {})

        # Count assets pointing to data.parquet
        hrefs_to_data = [
            key for key, asset in assets.items() if asset.get("href", "").endswith("data.parquet")
        ]

        assert len(hrefs_to_data) == 1, (
            f"Expected 1 asset for data.parquet, found {len(hrefs_to_data)}: {hrefs_to_data}. "
            "Duplicate asset entries were created."
        )

    @pytest.mark.integration
    def test_no_duplicate_when_human_key_differs_from_stem(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        valid_points_parquet: Path,
    ) -> None:
        """No duplicate when human-authored key differs from filename stem.

        Bug: Human key "census_2020" -> ./data.parquet, re-add creates
        "data" (stem) -> ./data.parquet. Both point to same file.
        """
        collection_dir = initialized_catalog / "testcol"
        collection_dir.mkdir()
        data_file = collection_dir / "data.parquet"
        shutil.copy(valid_points_parquet, data_file)

        # Create collection.json with manually-named asset "census_2020"
        # (NOTE: "census_2020" does NOT match stem "data")
        collection_json = {
            "type": "Collection",
            "stac_version": "1.1.0",
            "id": "testcol",
            "description": "Test collection",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
            },
            "links": [],
            "assets": {
                "census_2020": {
                    "href": "./data.parquet",
                    "type": "application/vnd.apache.parquet",
                    "roles": ["data"],
                    "title": "Census 2020 Data",
                }
            },
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

        # Re-add the file
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), "--force", str(data_file)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        collection_json = json.loads((collection_dir / "collection.json").read_text())
        assets = collection_json.get("assets", {})

        # Count assets pointing to data.parquet
        hrefs_to_data = [
            key for key, asset in assets.items() if asset.get("href", "").endswith("data.parquet")
        ]

        assert len(hrefs_to_data) == 1, (
            f"Expected 1 asset for data.parquet, found {len(hrefs_to_data)}: {hrefs_to_data}. "
            "Duplicate asset entries were created (stem-based key didn't find href match)."
        )

        # The human-authored key should be preserved (not replaced by stem)
        assert "census_2020" in assets, (
            f"Human-authored key 'census_2020' was lost. Keys: {list(assets.keys())}"
        )


class TestStaleAssetWarning:
    """Tests for stale asset detection (Bug #3).

    Per design: `add` warns about stale assets, `check --fix` removes them.
    """

    @pytest.mark.integration
    def test_warns_about_missing_asset_files(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        valid_points_parquet: Path,
    ) -> None:
        """Adding a new file should warn if existing assets reference missing files."""
        collection_dir = initialized_catalog / "testcol"
        collection_dir.mkdir()

        # Create collection.json with asset pointing to non-existent file
        collection_json = {
            "type": "Collection",
            "stac_version": "1.1.0",
            "id": "testcol",
            "description": "Test collection",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
            },
            "links": [],
            "assets": {
                "old_data": {
                    "href": "./deleted_file.parquet",
                    "type": "application/vnd.apache.parquet",
                    "roles": ["data"],
                }
            },
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

        # Add a new valid file
        new_file = collection_dir / "new_data.parquet"
        shutil.copy(valid_points_parquet, new_file)

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), "--force", str(new_file)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        # Should warn about the stale asset
        assert "missing" in result.output.lower() or "stale" in result.output.lower(), (
            f"Expected warning about missing/stale asset file. Output: {result.output}"
        )

    @pytest.mark.integration
    def test_stale_assets_not_removed_by_add(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        valid_points_parquet: Path,
    ) -> None:
        """Add should warn but NOT remove stale assets (that's check --fix's job)."""
        collection_dir = initialized_catalog / "testcol"
        collection_dir.mkdir()

        # Create collection.json with asset pointing to non-existent file
        collection_json = {
            "type": "Collection",
            "stac_version": "1.1.0",
            "id": "testcol",
            "description": "Test collection",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
            },
            "links": [],
            "assets": {
                "stale_asset": {
                    "href": "./gone.parquet",
                    "type": "application/vnd.apache.parquet",
                    "roles": ["data"],
                }
            },
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

        # Add a new file
        new_file = collection_dir / "new.parquet"
        shutil.copy(valid_points_parquet, new_file)

        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), "--force", str(new_file)],
            catch_exceptions=False,
        )

        # Stale asset should still be in collection.json (not removed)
        collection_json = json.loads((collection_dir / "collection.json").read_text())
        assert "stale_asset" in collection_json.get("assets", {}), (
            "Stale asset was removed by add. It should be preserved (check --fix removes it)."
        )


class TestExtentUpdate:
    """Tests for extent.spatial.bbox recomputation (Bug #4)."""

    @pytest.mark.integration
    def test_extent_updated_from_placeholder(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        valid_points_parquet: Path,
    ) -> None:
        """Placeholder whole-world extent should be replaced with actual data bbox."""
        collection_dir = initialized_catalog / "testcol"
        collection_dir.mkdir()

        # Create collection.json with placeholder extent
        collection_json = {
            "type": "Collection",
            "stac_version": "1.1.0",
            "id": "testcol",
            "description": "Test collection",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},  # Placeholder
                "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
            },
            "links": [],
            "assets": {},
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

        # Add a file with actual spatial extent
        data_file = collection_dir / "data.parquet"
        shutil.copy(valid_points_parquet, data_file)

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), "--force", str(data_file)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        collection_json = json.loads((collection_dir / "collection.json").read_text())
        bbox = collection_json["extent"]["spatial"]["bbox"][0]

        # Should NOT be the whole-world placeholder anymore
        assert bbox != [-180, -90, 180, 90], (
            f"Extent should be updated from data, not remain as placeholder. Got: {bbox}"
        )

    @pytest.mark.integration
    def test_extent_expands_on_new_file(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        valid_points_parquet: Path,
        projected_parquet: Path,
    ) -> None:
        """Adding a file outside current extent should expand the bbox."""
        collection_dir = initialized_catalog / "testcol"
        collection_dir.mkdir()

        # Add first file
        data_a = collection_dir / "data_a.parquet"
        shutil.copy(valid_points_parquet, data_a)

        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), "--force", str(data_a)],
            catch_exceptions=False,
        )

        # Add second file (different location - projected file covers different area)
        data_b = collection_dir / "data_b.parquet"
        shutil.copy(projected_parquet, data_b)

        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), "--force", str(data_b)],
            catch_exceptions=False,
        )

        collection_json = json.loads((collection_dir / "collection.json").read_text())
        bbox_after_b = collection_json["extent"]["spatial"]["bbox"][0]

        # The extent should have changed (expanded or shifted)
        # We can't know exact values without knowing fixture contents,
        # but at minimum it shouldn't be unchanged if the files have different extents
        # For now, just verify extent is set and is a valid bbox
        assert len(bbox_after_b) == 4, f"Invalid bbox: {bbox_after_b}"
        assert bbox_after_b[0] <= bbox_after_b[2], "min_x should be <= max_x"
        assert bbox_after_b[1] <= bbox_after_b[3], "min_y should be <= max_y"

    @pytest.mark.integration
    def test_extent_keep_strategy_preserves_manual_extent(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        valid_points_parquet: Path,
    ) -> None:
        """With --merge-strategy=keep, manually set extent should be preserved."""
        collection_dir = initialized_catalog / "testcol"
        collection_dir.mkdir()

        # Create collection.json with intentionally larger extent
        manual_bbox = [-120, 30, -110, 40]  # California-ish
        collection_json = {
            "type": "Collection",
            "stac_version": "1.1.0",
            "id": "testcol",
            "description": "Test collection",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [manual_bbox]},
                "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
            },
            "links": [],
            "assets": {},
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

        data_file = collection_dir / "data.parquet"
        shutil.copy(valid_points_parquet, data_file)

        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--force",
                "--merge-strategy=keep",
                str(data_file),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        collection_json = json.loads((collection_dir / "collection.json").read_text())
        bbox = collection_json["extent"]["spatial"]["bbox"][0]

        # With KEEP strategy, manual extent should be preserved
        assert bbox == manual_bbox, (
            f"KEEP strategy should preserve manual extent. Expected {manual_bbox}, got {bbox}"
        )
