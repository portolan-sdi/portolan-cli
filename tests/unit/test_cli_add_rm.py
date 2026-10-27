"""Unit tests for top-level add/rm commands.

Tests the CLI layer for `portolan add` and `portolan rm` commands.
These commands replace the old `dataset add` and `dataset remove` subcommands.

Git-style implicit tracking
- `add <path>` tracks files (infers collection from path)
- `rm <path>` untracks AND deletes (no confirmation)
- `rm --keep <path>` untracks but preserves file
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from portolan_cli.add import ItemInfo
from portolan_cli.cli import cli

if TYPE_CHECKING:
    from tests.catalog_invariants import RunCli

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
# `add --item-id` writes asset hrefs relative to a directory that does not
# exist, so run_cli must not check those catalogs at teardown.


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI test runner."""
    return CliRunner()


def setup_catalog(path: Path) -> None:
    """Create an initialized Portolan catalog ( and.

    Creates both .portolan/config.yaml (the sentinel) and catalog.json.
    """
    portolan_dir = path / ".portolan"
    portolan_dir.mkdir()
    # Create config.yaml as sentinel
    (portolan_dir / "config.yaml").write_text("# Portolan configuration\n")
    # Create catalog.json at root (STAC standard)
    catalog_data = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "portolan-catalog",
        "description": "A Portolan-managed STAC catalog",
        "links": [],
    }
    (path / "catalog.json").write_text(json.dumps(catalog_data, indent=2))


class TestAdd:
    """Tests for 'portolan add' command."""

    @pytest.mark.unit
    def test_add_single_file(self, runner: CliRunner) -> None:
        """add single file tracks it and infers collection from path."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            # Create test file in collection directory
            collection_dir = temp_path / "demographics"
            collection_dir.mkdir()
            test_file = collection_dir / "census.geojson"
            test_file.write_text('{"type": "FeatureCollection", "features": []}')

            with patch("portolan_cli.add.add_files") as mock_add:
                mock_add.return_value = (
                    [
                        ItemInfo(
                            item_id="census",
                            collection_id="demographics",
                            format_type="vector",
                            bbox=[-122.5, 37.5, -122.0, 38.0],
                            asset_paths=["census.parquet"],
                        )
                    ],
                    [],  # skipped
                    [],  # failures
                )

                result = runner.invoke(
                    cli,
                    ["add", str(test_file)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                mock_add.assert_called_once()

    @pytest.mark.unit
    def test_add_infers_collection_from_path(self, runner: CliRunner) -> None:
        """add passes collection_id=None and delegates per-file inference to add_files."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            # Create test file in nested structure
            collection_dir = temp_path / "imagery"
            collection_dir.mkdir()
            test_file = collection_dir / "satellite.tif"
            test_file.write_bytes(b"GeoTIFF content")

            with patch("portolan_cli.add.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                runner.invoke(
                    cli,
                    ["add", str(test_file)],
                    catch_exceptions=False,
                )

                # The CLI now always passes collection_id=None; add_files infers per-file.
                call_args = mock_add.call_args
                assert call_args is not None
                assert call_args.kwargs.get("collection_id") is None
                assert test_file.resolve() in call_args.kwargs["paths"]

    @pytest.mark.unit
    def test_add_directory(self, runner: CliRunner) -> None:
        """add directory adds all files inside."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            # Create directory with multiple files
            collection_dir = temp_path / "vectors"
            collection_dir.mkdir()
            (collection_dir / "file1.geojson").write_text("{}")
            (collection_dir / "file2.geojson").write_text("{}")

            with patch("portolan_cli.add.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                result = runner.invoke(
                    cli,
                    ["add", str(collection_dir)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                mock_add.assert_called_once()

    @pytest.mark.unit
    def test_add_skips_unchanged_silently(self, runner: CliRunner) -> None:
        """add skips unchanged files without output (unless --verbose)."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "existing.geojson"
            test_file.write_text("{}")

            with patch("portolan_cli.add.add_files") as mock_add:
                # Return empty list to indicate nothing was added (all unchanged)
                mock_add.return_value = ([], [test_file], [])

                result = runner.invoke(
                    cli,
                    ["add", str(test_file)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                # Output should be minimal for unchanged files
                assert "existing" not in result.output or "unchanged" not in result.output.lower()

    @pytest.mark.unit
    def test_add_verbose_shows_skipped(self, runner: CliRunner) -> None:
        """add --verbose shows skipped files."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "existing.geojson"
            test_file.write_text("{}")

            with patch("portolan_cli.add.add_files") as mock_add:
                mock_add.return_value = ([], [test_file], [])

                result = runner.invoke(
                    cli,
                    ["add", "--verbose", str(test_file)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0

    @pytest.mark.unit
    def test_add_nonexistent_path(self, runner: CliRunner) -> None:
        """add fails with error for nonexistent path."""
        result = runner.invoke(cli, ["add", "/nonexistent/path"])

        assert result.exit_code != 0
        # Click should report the path doesn't exist
        assert "does not exist" in result.output.lower() or "not found" in result.output.lower()

    @pytest.mark.unit
    def test_add_not_a_catalog_fails(self, runner: CliRunner) -> None:
        """add fails when not in a catalog directory."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)

            # Don't create catalog - just a regular directory
            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "test.geojson"
            test_file.write_text("{}")

            result = runner.invoke(cli, ["add", str(test_file)])

            assert result.exit_code == 1
            # Error message references .portolan/config.yaml sentinel
            assert "no .portolan/config.yaml found" in result.output.lower()

    @pytest.mark.unit
    def test_add_raster_at_collection_root_reports_specific_reason(self, runner: CliRunner) -> None:
        """A raster file at <collection>/file.tif (no item subdir) is skipped,
        and the warning identifies the specific rule — *raster needs item
        subdirectory* — rather than the misleading generic 'catalog root'
        message that the warn used to print for all infer-id failures.
        """
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "elevation"
            collection_dir.mkdir()
            # TIF directly under collection/, no item subdirectory
            (collection_dir / "scene.tif").write_bytes(b"")

            result = runner.invoke(
                cli,
                ["add", "elevation/"],
                catch_exceptions=False,
            )

            # The file is skipped, but the message must point at the actual
            # rule: raster needs collection/item/ structure.
            assert "Skipping" in result.output
            assert "scene.tif" in result.output
            # Specific reason from infer_nested_collection_id, not the old
            # generic "files at catalog root" wording.
            assert (
                "collection/item" in result.output
                or "Raster" in result.output
                or "item-level" in result.output
            ), f"warning should explain the raster item-subdir rule; got:\n{result.output}"

    @pytest.mark.integration
    def test_add_with_item_id_override(
        self,
        tmp_path: Path,
        run_cli: RunCli,
    ) -> None:
        """add --item-id overrides automatic item ID derivation."""
        root = tmp_path / "catalog"
        collection_dir = root / "demographics"
        collection_dir.mkdir(parents=True)
        test_file = collection_dir / "census.geojson"
        shutil.copy(FIXTURES / "vector" / "valid" / "points.geojson", test_file)
        run_cli("init", str(root), "--auto", "--license", "CC-BY-4.0")

        result = run_cli(
            "--format", "json", "add", "--item-id", "custom-item-id", str(test_file), cwd=root
        )

        added = json.loads(result.stdout)["data"]["added"]
        assert [(a["item_id"], a["collection_id"]) for a in added] == [
            ("custom-item-id", "demographics")
        ]
        item = json.loads((collection_dir / "custom-item-id.json").read_text(encoding="utf-8"))
        assert item["id"] == "custom-item-id"

    @pytest.mark.integration
    def test_add_item_id_passed_to_add_files(
        self,
        tmp_path: Path,
        run_cli: RunCli,
    ) -> None:
        """--item-id reaches add_files: the raster item takes the given id."""
        root = tmp_path / "catalog"
        item_dir = root / "imagery" / "scene"
        item_dir.mkdir(parents=True)
        test_file = item_dir / "satellite.tif"
        shutil.copy(FIXTURES / "raster" / "valid" / "singleband.tif", test_file)
        run_cli("init", str(root), "--auto", "--license", "CC-BY-4.0")

        run_cli("add", "--item-id", "my-custom-id", str(test_file), cwd=root)

        item = json.loads((item_dir / "my-custom-id.json").read_text(encoding="utf-8"))
        assert item["id"] == "my-custom-id"
        assert not (item_dir / "scene.json").exists()

    @pytest.mark.unit
    def test_add_without_item_id_passes_none(self, runner: CliRunner) -> None:
        """Without --item-id, add_files receives item_id=None (auto-derive)."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "vectors"
            collection_dir.mkdir()
            test_file = collection_dir / "data.geojson"
            test_file.write_text("{}")

            with patch("portolan_cli.add.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                runner.invoke(
                    cli,
                    ["add", str(test_file)],
                    catch_exceptions=False,
                )

                call_args = mock_add.call_args
                assert call_args is not None
                assert call_args.kwargs.get("item_id") is None

    @pytest.mark.unit
    def test_add_item_id_with_directory_fails(self, tmp_path: Path, run_cli: RunCli) -> None:
        """--item-id with a directory path should fail (ambiguous for multiple files)."""
        setup_catalog(tmp_path)
        collection_dir = tmp_path / "demographics"
        collection_dir.mkdir()

        result = run_cli("add", "--item-id", "my-id", str(collection_dir), expect=1)

        assert "--item-id can only be used with a single file, not a directory" in result.output

    @pytest.mark.unit
    def test_add_multiple_paths(self, runner: CliRunner) -> None:
        """add accepts multiple paths and batches them into a single add_files call (Issue #176)."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            # Create multiple files in different collections
            coll1 = temp_path / "collection1"
            coll1.mkdir()
            file1 = coll1 / "data1.geojson"
            file1.write_text("{}")

            coll2 = temp_path / "collection2"
            coll2.mkdir()
            file2 = coll2 / "data2.geojson"
            file2.write_text("{}")

            with patch("portolan_cli.add.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                result = runner.invoke(
                    cli,
                    ["add", str(file1), str(file2)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                # Should be called ONCE with all paths batched together
                mock_add.assert_called_once()
                call_args = mock_add.call_args
                assert call_args is not None
                # Both resolved paths should be in the single call
                passed_paths = call_args.kwargs["paths"]
                assert file1.resolve() in passed_paths
                assert file2.resolve() in passed_paths

    @pytest.mark.unit
    def test_add_multiple_paths_mixed_collections(self, runner: CliRunner) -> None:
        """add multiple paths from different collections uses a single add_files call with collection_id=None."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            # Create files in different collections
            demographics = temp_path / "demographics"
            demographics.mkdir()
            census = demographics / "census.geojson"
            census.write_text("{}")

            imagery = temp_path / "imagery"
            imagery.mkdir()
            satellite = imagery / "satellite.tif"
            satellite.write_bytes(b"tiff")

            with patch("portolan_cli.add.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                runner.invoke(
                    cli,
                    ["add", str(census), str(satellite)],
                    catch_exceptions=False,
                )

                # Should be called ONCE with collection_id=None (add_files infers per-file)
                mock_add.assert_called_once()
                call_args = mock_add.call_args
                assert call_args is not None
                assert call_args.kwargs.get("collection_id") is None
                # Both files should be in the paths list
                passed_paths = call_args.kwargs["paths"]
                assert census.resolve() in passed_paths
                assert satellite.resolve() in passed_paths

    @pytest.mark.unit
    def test_add_multiple_paths_reports_combined_results(self, runner: CliRunner) -> None:
        """add multiple paths from the same collection shows single-collection output."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            coll = temp_path / "data"
            coll.mkdir()
            file1 = coll / "data1.geojson"
            file1.write_text("{}")
            file2 = coll / "data2.geojson"
            file2.write_text("{}")

            with patch("portolan_cli.add.add_files") as mock_add:
                # Two items added from the same collection
                mock_add.return_value = (
                    [
                        ItemInfo(
                            item_id="item1",
                            collection_id="data",
                            format_type="vector",
                            bbox=[0, 0, 1, 1],
                            asset_paths=["data1.parquet"],
                        ),
                        ItemInfo(
                            item_id="item2",
                            collection_id="data",
                            format_type="vector",
                            bbox=[0, 0, 1, 1],
                            asset_paths=["data2.parquet"],
                        ),
                    ],
                    [],
                    [],
                )

                result = runner.invoke(
                    cli,
                    ["add", str(file1), str(file2)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                # Summary-only output, shows file and collection count
                # Collection name only shown with --verbose
                assert "2 files" in result.output
                assert "1 collection" in result.output

    @pytest.mark.unit
    def test_add_multiple_paths_failure_on_nth_path(self, runner: CliRunner) -> None:
        """If add_files raises an error, exit code is non-zero and a useful message is shown."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            coll = temp_path / "data"
            coll.mkdir()
            file1 = coll / "data1.geojson"
            file1.write_text("{}")
            file2 = coll / "data2.geojson"
            file2.write_text("{}")

            with patch("portolan_cli.add.add_files") as mock_add:
                mock_add.side_effect = ValueError("invalid geometry")

                result = runner.invoke(
                    cli,
                    ["add", str(file1), str(file2)],
                )

                assert result.exit_code != 0
                assert "invalid geometry" in result.output

    @pytest.mark.unit
    def test_add_duplicate_paths_deduplicates(self, runner: CliRunner) -> None:
        """Passing the same path twice should result in a single resolved path in the batch."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            coll = temp_path / "data"
            coll.mkdir()
            file1 = coll / "data1.geojson"
            file1.write_text("{}")

            with patch("portolan_cli.add.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                result = runner.invoke(
                    cli,
                    ["add", str(file1), str(file1)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                # add_files should be called once with deduplicated paths
                mock_add.assert_called_once()
                passed_paths = mock_add.call_args.kwargs["paths"]
                # The same resolved path should appear only once
                assert passed_paths.count(file1.resolve()) == 1

    @pytest.mark.unit
    def test_add_symlink_resolves_correctly(self, runner: CliRunner) -> None:
        """Symlinks in multi-path add are resolved to real paths before deduplication."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            coll = temp_path / "data"
            coll.mkdir()
            real_file = coll / "data1.geojson"
            real_file.write_text("{}")
            symlink_file = coll / "link_to_data1.geojson"
            symlink_file.symlink_to(real_file)

            with patch("portolan_cli.add.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                result = runner.invoke(
                    cli,
                    ["add", str(real_file), str(symlink_file)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                # Both paths resolve to the same file, so only one path in batch
                mock_add.assert_called_once()
                passed_paths = mock_add.call_args.kwargs["paths"]
                assert len(passed_paths) == 1
                assert passed_paths[0] == real_file.resolve()

    @pytest.mark.unit
    def test_add_mixed_relative_absolute_paths(self, runner: CliRunner) -> None:
        """Mixed relative and absolute paths are both resolved and batched correctly."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            coll = temp_path / "data"
            coll.mkdir()
            file1 = coll / "data1.geojson"
            file1.write_text("{}")
            file2 = coll / "data2.geojson"
            file2.write_text("{}")

            with patch("portolan_cli.add.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                # Pass one absolute path and one path that Click will resolve
                result = runner.invoke(
                    cli,
                    ["add", str(file1.resolve()), str(file2)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                mock_add.assert_called_once()
                passed_paths = mock_add.call_args.kwargs["paths"]
                assert file1.resolve() in passed_paths
                assert file2.resolve() in passed_paths

    @pytest.mark.unit
    def test_add_no_paths_fails(self, runner: CliRunner) -> None:
        """add without any paths fails with error."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            result = runner.invoke(cli, ["add"])

            # Click should report missing argument
            assert result.exit_code != 0
            assert "missing argument" in result.output.lower() or "paths" in result.output.lower()

    @pytest.mark.unit
    def test_add_item_id_with_multiple_paths_fails(self, tmp_path: Path, run_cli: RunCli) -> None:
        """--item-id with multiple paths fails (ambiguous)."""
        setup_catalog(tmp_path)
        coll = tmp_path / "data"
        coll.mkdir()
        file1 = coll / "data1.geojson"
        file1.write_text("{}")
        file2 = coll / "data2.geojson"
        file2.write_text("{}")

        result = run_cli("add", "--item-id", "my-id", str(file1), str(file2), expect=1)

        # Should reject using --item-id with multiple paths
        assert "--item-id can only be used with a single file, not multiple paths" in result.output


class TestRm:
    """Tests for 'portolan rm' command."""

    @pytest.mark.unit
    def test_rm_requires_force_for_destructive(self, runner: CliRunner) -> None:
        """rm without --force or --keep fails with safety error."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "to_remove.parquet"
            test_file.write_bytes(b"parquet data")

            result = runner.invoke(cli, ["rm", str(test_file)])

            assert result.exit_code == 1
            assert "--force" in result.output or "SafetyError" in result.output

    @pytest.mark.unit
    def test_rm_force_deletes_and_untracks(self, runner: CliRunner) -> None:
        """rm --force deletes file and removes from tracking."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "to_remove.parquet"
            test_file.write_bytes(b"parquet data")

            with patch("portolan_cli.cli.remove_files") as mock_rm:
                mock_rm.return_value = ([test_file], [])  # (removed, skipped)

                result = runner.invoke(
                    cli,
                    ["rm", "--force", str(test_file)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                mock_rm.assert_called_once()
                call_kwargs = mock_rm.call_args.kwargs
                assert call_kwargs.get("keep") is False
                assert call_kwargs.get("dry_run") is False

    @pytest.mark.unit
    def test_rm_keep_does_not_require_force(self, runner: CliRunner) -> None:
        """rm --keep does not require --force (safe operation)."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "file.parquet"
            test_file.write_bytes(b"data")

            with patch("portolan_cli.cli.remove_files") as mock_rm:
                mock_rm.return_value = ([test_file], [])

                # No --force needed with --keep
                result = runner.invoke(
                    cli,
                    ["rm", "--keep", str(test_file)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                mock_rm.assert_called_once()

    @pytest.mark.unit
    def test_rm_keep_preserves_file(self, runner: CliRunner) -> None:
        """rm --keep untracks but preserves the file."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "keep_me.parquet"
            test_file.write_bytes(b"data")

            with patch("portolan_cli.cli.remove_files") as mock_rm:
                mock_rm.return_value = ([test_file], [])

                result = runner.invoke(
                    cli,
                    ["rm", "--keep", str(test_file)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                mock_rm.assert_called_once()
                call_kwargs = mock_rm.call_args.kwargs
                assert call_kwargs.get("keep") is True

    @pytest.mark.unit
    def test_rm_dry_run_does_not_require_force(self, runner: CliRunner) -> None:
        """rm --dry-run does not require --force (safe operation)."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "file.parquet"
            test_file.write_bytes(b"data")

            with patch("portolan_cli.cli.remove_files") as mock_rm:
                mock_rm.return_value = ([test_file], [])

                result = runner.invoke(
                    cli,
                    ["rm", "--dry-run", str(test_file)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                assert "dry run" in result.output.lower() or "would remove" in result.output.lower()
                mock_rm.assert_called_once()
                call_kwargs = mock_rm.call_args.kwargs
                assert call_kwargs.get("dry_run") is True

    @pytest.mark.unit
    def test_rm_missing_path_is_not_rejected_at_the_cli(self, runner: CliRunner) -> None:
        """rm no longer rejects a path missing from disk at the CLI boundary (#803).

        Before the fix, click's ``Path(exists=True)`` blocked rm on a deleted
        file. Now a missing, untracked path reaches ``remove_files`` and is
        skipped cleanly rather than failing with "does not exist".
        """
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()

            result = runner.invoke(
                cli,
                ["rm", "--force", str(collection_dir / "gone.parquet")],
                catch_exceptions=False,
            )

            assert result.exit_code == 0
            assert "does not exist" not in result.output
            assert "Removed" not in result.output
            # A path that is neither tracked nor on disk is a no-op. rm must
            # report it, not exit silently (#827).
            assert "Nothing to remove" in result.output
            assert "gone.parquet" in result.output

    @pytest.mark.unit
    def test_rm_directory_with_force(self, runner: CliRunner) -> None:
        """rm --force can remove entire directory."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "to_remove"
            collection_dir.mkdir()
            (collection_dir / "file1.parquet").write_bytes(b"data1")
            (collection_dir / "file2.parquet").write_bytes(b"data2")

            with patch("portolan_cli.cli.remove_files") as mock_rm:
                mock_rm.return_value = ([], [])

                result = runner.invoke(
                    cli,
                    ["rm", "--force", str(collection_dir)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                mock_rm.assert_called_once()

    @pytest.mark.unit
    def test_rm_verbose_shows_skipped(self, runner: CliRunner) -> None:
        """rm --verbose shows skipped files."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "file.parquet"
            test_file.write_bytes(b"data")
            skipped_file = collection_dir / "outside.parquet"

            with patch("portolan_cli.cli.remove_files") as mock_rm:
                mock_rm.return_value = ([test_file], [skipped_file])

                result = runner.invoke(
                    cli,
                    ["rm", "--force", "--verbose", str(test_file)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                # Should show skipped file in verbose mode
                assert "outside" in result.output.lower() or "skipped" in result.output.lower()


class TestAddSidecarDetection:
    """Tests for sidecar auto-detection in add command."""

    @pytest.mark.unit
    def test_add_shapefile_includes_sidecars(self, runner: CliRunner) -> None:
        """add .shp automatically includes sidecar files."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "vectors"
            collection_dir.mkdir()

            # Create shapefile with sidecars
            (collection_dir / "data.shp").write_bytes(b"shp")
            (collection_dir / "data.dbf").write_bytes(b"dbf")
            (collection_dir / "data.shx").write_bytes(b"shx")
            (collection_dir / "data.prj").write_text("EPSG:4326")

            with patch("portolan_cli.add.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                result = runner.invoke(
                    cli,
                    ["add", str(collection_dir / "data.shp")],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0

    @pytest.mark.unit
    def test_add_tiff_includes_worldfile(self, runner: CliRunner) -> None:
        """add .tif automatically includes .tfw world file."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "imagery"
            collection_dir.mkdir()

            # Create TIFF with world file
            (collection_dir / "image.tif").write_bytes(b"tiff")
            (collection_dir / "image.tfw").write_text("1.0\n0.0\n0.0\n-1.0\n0.0\n0.0")

            with patch("portolan_cli.add.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                result = runner.invoke(
                    cli,
                    ["add", str(collection_dir / "image.tif")],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0


class TestPathToCollectionResolution:
    """Tests for path -> collection ID resolution.

    With the batched add_files approach (CLI wraps API), the CLI now
    delegates collection inference entirely to add_files by passing collection_id=None.
    These tests verify that paths are correctly resolved and passed through.
    """

    @pytest.mark.unit
    def test_resolve_collection_from_nested_path(self, runner: CliRunner) -> None:
        """Nested path is resolved and passed to add_files with collection_id=None."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            # Create nested structure: catalog/demographics/2020/census.geojson
            nested_dir = temp_path / "demographics" / "2020"
            nested_dir.mkdir(parents=True)
            test_file = nested_dir / "census.geojson"
            test_file.write_text("{}")

            with patch("portolan_cli.add.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                runner.invoke(
                    cli,
                    ["add", str(test_file)],
                    catch_exceptions=False,
                )

                call_args = mock_add.call_args
                assert call_args is not None
                # The CLI delegates collection inference to add_files (collection_id=None)
                assert call_args.kwargs.get("collection_id") is None
                assert test_file.resolve() in call_args.kwargs["paths"]

    @pytest.mark.unit
    def test_resolve_collection_from_direct_child(self, runner: CliRunner) -> None:
        """Direct child path is resolved and passed to add_files with collection_id=None."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "imagery"
            collection_dir.mkdir()
            test_file = collection_dir / "satellite.tif"
            test_file.write_bytes(b"tiff")

            with patch("portolan_cli.add.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                runner.invoke(
                    cli,
                    ["add", str(test_file)],
                    catch_exceptions=False,
                )

                call_args = mock_add.call_args
                assert call_args is not None
                # The CLI delegates collection inference to add_files (collection_id=None)
                assert call_args.kwargs.get("collection_id") is None
                assert test_file.resolve() in call_args.kwargs["paths"]


class TestAddJsonOutput:
    """Stdout of a JSON add is one JSON document (#1005).

    Each test runs the real add. A library message that reaches stdout
    breaks ``json.loads(result.stdout)``.
    """

    @pytest.mark.integration
    @pytest.mark.realdata
    @pytest.mark.parametrize(
        "flags",
        [["--format", "json", "add"], ["add", "--json"]],
        ids=["format-json", "json-flag"],
    )
    def test_add_json_of_a_rewritten_geoparquet(
        self,
        run_cli: RunCli,
        flags: list[str],
        conformant_catalog: Path,
        open_buildings_path: Path,
    ) -> None:
        """A GeoParquet with no bbox covering column is rewritten, and stdout stays JSON."""
        root = conformant_catalog
        (root / "buildings").mkdir()
        (root / "buildings" / open_buildings_path.name).write_bytes(
            open_buildings_path.read_bytes()
        )

        result = run_cli(*flags, "--portolan-dir", str(root), str(root / "buildings"))

        envelope = json.loads(result.stdout)
        assert envelope["success"] is True
        assert envelope["command"] == "add"
        assert [item["item_id"] for item in envelope["data"]["added"]] == ["open-buildings"]
        assert (
            "→ Rewriting open-buildings.parquet (142.4KB): it carries no bbox covering column\n"
        ) in result.stderr

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "flags",
        [["--format", "json", "add"], ["add", "--json"]],
        ids=["format-json", "json-flag"],
    )
    def test_library_detail_goes_to_stderr(
        self,
        run_cli: RunCli,
        flags: list[str],
        conformant_catalog: Path,
        valid_points_geojson: Path,
    ) -> None:
        """The thumbnail line of ``add --verbose`` goes to stderr in JSON mode."""
        root = conformant_catalog
        (root / "collection").mkdir()
        (root / "collection" / "good.geojson").write_bytes(valid_points_geojson.read_bytes())

        result = run_cli(
            *[*flags, "--verbose", "--portolan-dir", str(root), str(root / "collection")]
        )

        assert json.loads(result.stdout)["success"] is True
        assert "  Rendered thumbnail for collection: good.thumb.jpg\n" in result.stderr


class TestAddWorkerNotice:
    """The worker count is log chatter, not a user-facing message."""

    @pytest.mark.integration
    def test_worker_count_is_an_info_log_record(
        self,
        run_cli: RunCli,
        conformant_catalog: Path,
        valid_points_geojson: Path,
    ) -> None:
        """Text mode keeps stdout free of it. ``-v`` shows it on stderr as a log record."""
        root = conformant_catalog
        (root / "collection").mkdir()
        for name in ("a.geojson", "b.geojson"):
            (root / "collection" / name).write_bytes(valid_points_geojson.read_bytes())
        args = [
            "add",
            "--workers",
            "2",
            "--portolan-dir",
            str(root),
            str(root / "collection"),
        ]

        result = run_cli("-v", *args)

        assert "parallel workers" not in result.stdout
        assert "INFO portolan_cli.add: Using 2 parallel workers for 2 files\n" in result.stderr


class TestAddIgnoredFiles:
    """``add`` skips the files that ``ignored_files`` in config.yaml excludes (#1044).

    Every skipped file keeps its reason, in the human summary, on stderr, and
    in the JSON envelope.
    """

    IGNORED = "it matches ignored_files pattern '*.geojson'"
    NOTICE = f"⚠ Skipping places.geojson: {IGNORED}\n"

    @staticmethod
    def _catalog(root: Path, geojson: Path, parquet: Path | None) -> Path:
        config = root / ".portolan" / "config.yaml"
        config.write_text(config.read_text() + 'ignored_files:\n  - "*.geojson"\n')
        (root / "places").mkdir()
        (root / "places" / "places.geojson").write_bytes(geojson.read_bytes())
        if parquet is not None:
            (root / "roads").mkdir()
            (root / "roads" / "roads.parquet").write_bytes(parquet.read_bytes())
        return root / "places" / "places.geojson"

    @pytest.mark.integration
    @pytest.mark.parametrize("json_mode", [False, True], ids=["text", "json"])
    def test_an_ignored_file_is_skipped_with_the_reason(
        self,
        run_cli: RunCli,
        conformant_catalog: Path,
        valid_points_geojson: Path,
        valid_points_parquet: Path,
        json_mode: bool,
    ) -> None:
        """The ignored GeoJSON stays out of the catalog. The kept GeoParquet goes in."""
        root = conformant_catalog
        places = self._catalog(root, valid_points_geojson, valid_points_parquet)
        prefix = ["--format", "json"] if json_mode else []

        result = run_cli(*prefix, "add", "--portolan-dir", str(root), str(root))

        assert self.NOTICE in result.stderr
        assert not (root / "places" / "collection.json").exists()
        assert not (root / "places" / "places.parquet").exists()
        assert (root / "roads" / "collection.json").exists()
        if json_mode:
            envelope = json.loads(result.stdout)
            assert [item["item_id"] for item in envelope["data"]["added"]] == ["roads"]
            assert envelope["data"]["skipped"] == [{"path": str(places), "reason": self.IGNORED}]

    @pytest.mark.integration
    def test_an_ignored_sibling_stays_out_of_the_assets(
        self,
        run_cli: RunCli,
        conformant_catalog: Path,
        valid_points_geojson: Path,
        valid_points_parquet: Path,
    ) -> None:
        """An ignored file beside an added file is not scanned in as its companion."""
        root = conformant_catalog
        self._catalog(root, valid_points_geojson, valid_points_parquet)
        (root / "roads" / "notes.geojson").write_bytes(valid_points_geojson.read_bytes())

        run_cli("add", "--portolan-dir", str(root), str(root / "roads" / "roads.parquet"))

        collection = json.loads((root / "roads" / "collection.json").read_text())
        hrefs = sorted(asset["href"] for asset in collection["assets"].values())
        assert hrefs == ["./roads.parquet", "./roads.thumb.jpg"]
        versions = json.loads((root / "roads" / "versions.json").read_text())
        assert sorted(versions["versions"][-1]["assets"]) == ["roads.parquet", "roads.thumb.jpg"]

    @pytest.mark.integration
    def test_a_source_whose_output_is_ignored_is_skipped_before_conversion(
        self, run_cli: RunCli, conformant_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """A pattern that excludes the converted file rejects the add of its source.

        Without this, the GeoJSON passes the input filter, its GeoParquet is
        written and then dropped by the asset scan, and the catalog publishes
        the source with the data role.
        """
        root = conformant_catalog
        config = root / ".portolan" / "config.yaml"
        config.write_text(config.read_text() + 'ignored_files:\n  - "*.parquet"\n')
        (root / "places").mkdir()
        places = root / "places" / "places.geojson"
        places.write_bytes(valid_points_geojson.read_bytes())

        result = run_cli("--format", "json", "add", "--portolan-dir", str(root), str(root))

        assert json.loads(result.stdout)["data"]["skipped"] == [
            {
                "path": str(places),
                "reason": "its output places.parquet matches ignored_files pattern '*.parquet'",
            }
        ]
        assert not (root / "places" / "collection.json").exists()
        assert not (root / "places" / "places.parquet").exists()

    @pytest.mark.integration
    def test_a_run_that_only_skips_names_the_reason(
        self, run_cli: RunCli, conformant_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """The issue's reproduction: the summary does not call an ignored file tracked."""
        root = conformant_catalog
        self._catalog(root, valid_points_geojson, None)

        result = run_cli("add", "--verbose", "--portolan-dir", str(root), str(root))

        assert self.NOTICE in result.stderr
        assert "already tracked" not in result.output
        assert f"→ Nothing added: 1 file skipped\n    places.geojson: {self.IGNORED}\n" in (
            result.stdout
        )

    @pytest.mark.integration
    def test_a_second_add_gives_each_skip_its_own_reason(
        self,
        run_cli: RunCli,
        conformant_catalog: Path,
        valid_points_geojson: Path,
        valid_points_parquet: Path,
    ) -> None:
        """An unchanged file and an ignored file carry different reasons."""
        root = conformant_catalog
        places = self._catalog(root, valid_points_geojson, valid_points_parquet)
        args = ["--format", "json", "add", "--portolan-dir", str(root), str(root)]
        run_cli(*args)

        result = run_cli(*args)

        skipped = sorted(json.loads(result.stdout)["data"]["skipped"], key=lambda s: s["path"])
        assert skipped == [
            {"path": str(places), "reason": self.IGNORED},
            {
                "path": str(root / "roads" / "roads.parquet"),
                "reason": "unchanged since the last add",
            },
        ]

    @pytest.mark.integration
    def test_an_ignored_files_value_that_is_not_a_list_fails_the_add(
        self, run_cli: RunCli, conformant_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """A malformed ``ignored_files`` is a typed error envelope, not a traceback."""
        root = conformant_catalog
        config = root / ".portolan" / "config.yaml"
        config.write_text(config.read_text() + 'ignored_files: "*.geojson"\n')
        (root / "places").mkdir()
        (root / "places" / "places.geojson").write_bytes(valid_points_geojson.read_bytes())

        result = run_cli(
            "--format", "json", "add", "--portolan-dir", str(root), str(root), expect=1
        )

        errors = json.loads(result.stdout)["errors"]
        assert [e["type"] for e in errors] == ["ConfigInvalidStructureError"]
        assert "'ignored_files' must be a list of glob patterns (strings)" in errors[0]["message"]

    @pytest.mark.integration
    def test_the_skip_summary_counts_in_the_plural(
        self, run_cli: RunCli, conformant_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """Two ignored files read as "2 files", each with its own line."""
        root = conformant_catalog
        self._catalog(root, valid_points_geojson, None)
        (root / "places" / "parks.geojson").write_bytes(valid_points_geojson.read_bytes())

        result = run_cli("add", "--verbose", "--portolan-dir", str(root), str(root))

        assert "→ Nothing added: 2 files skipped\n" in result.stdout
        assert f"    parks.geojson: {self.IGNORED}\n" in result.stdout
        assert f"    places.geojson: {self.IGNORED}\n" in result.stdout

    @pytest.mark.integration
    def test_verbose_names_each_skip_beside_an_added_file(
        self,
        run_cli: RunCli,
        conformant_catalog: Path,
        valid_points_geojson: Path,
        valid_points_parquet: Path,
    ) -> None:
        """When a run adds files, ``--verbose`` still gives each skip its reason."""
        root = conformant_catalog
        self._catalog(root, valid_points_geojson, valid_points_parquet)
        run_cli("add", "--portolan-dir", str(root), str(root))
        (root / "trails").mkdir()
        (root / "trails" / "trails.parquet").write_bytes(valid_points_parquet.read_bytes())

        result = run_cli("add", "--verbose", "--portolan-dir", str(root), str(root))

        assert "  Skipping roads.parquet (unchanged since the last add)\n" in result.stdout
        assert f"  Skipping places.geojson ({self.IGNORED})\n" in result.stdout
        assert "✓ Added 1 file to 1 collection\n" in result.stdout


class TestReportSkipsAndFindCollections:
    """Skips that stay in the catalog keep their collection affected. Others warn."""

    @pytest.mark.unit
    def test_an_ignored_file_before_a_tracked_skip(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """The warning for the ignored file does not stop the walk over the later skips.

        A tracked skip belongs to the nearest collection above it, which can
        sit below a subcatalog.
        """
        from portolan_cli.cli import _report_skips_and_find_collections

        for collection in ("roads", "parks", "region/trails"):
            (tmp_path / collection).mkdir(parents=True)
            (tmp_path / collection / "collection.json").write_text("{}")
        skips = [
            (tmp_path / "places" / "places.geojson", "it matches ignored_files pattern '*'"),
            (tmp_path / "roads" / "scene" / "roads.tif", "unchanged since the last add"),
            (tmp_path / "parks" / "items.parquet", "it is the STAC-GeoParquet item mirror"),
            (tmp_path / "region" / "trails" / "trails.parquet", "unchanged since the last add"),
            (tmp_path.parent / "elsewhere.parquet", "unchanged since the last add"),
        ]

        affected, versioned = _report_skips_and_find_collections([], skips, tmp_path)

        assert (affected, versioned) == ({"roads", "parks", "region/trails"}, set())
        assert capsys.readouterr().err == (
            "⚠ Skipping places.geojson: it matches ignored_files pattern '*'\n"
        )

    @pytest.mark.integration
    def test_a_second_add_restores_the_thumbnail_of_a_nested_collection(
        self, run_cli: RunCli, conformant_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """An unchanged file keeps its nested collection in the side steps of the add."""
        root = conformant_catalog
        collection_dir = root / "region" / "roads"
        collection_dir.mkdir(parents=True)
        (collection_dir / "roads.geojson").write_bytes(valid_points_geojson.read_bytes())
        args = ["add", "--portolan-dir", str(root), str(root)]
        run_cli(*args)
        thumbnail = collection_dir / "roads.thumb.jpg"
        thumbnail.unlink()
        collection_file = collection_dir / "collection.json"
        collection = json.loads(collection_file.read_text(encoding="utf-8"))
        del collection["assets"]["thumbnail"]
        collection_file.write_text(json.dumps(collection), encoding="utf-8")

        run_cli(*args)

        collection = json.loads(collection_file.read_text(encoding="utf-8"))
        assert collection["assets"]["thumbnail"]["href"] == "./roads.thumb.jpg"
        assert thumbnail.is_file()


class TestOutputAddUnchanged:
    """A run that adds nothing calls the files tracked only when all of them are."""

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("reasons", "summary"),
        [
            (
                ["unchanged since the last add", "it is the STAC-GeoParquet item mirror"],
                "✓ All 2 files already tracked (unchanged)\n",
            ),
            (
                ["unchanged since the last add", "it matches ignored_files pattern '*'"],
                "→ Nothing added: 2 files skipped\n",
            ),
        ],
        ids=["tracked", "ignored"],
    )
    def test_summary(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str], reasons: list[str], summary: str
    ) -> None:
        from portolan_cli.cli import _output_add_unchanged

        skips = [(tmp_path / f"{index}.parquet", reason) for index, reason in enumerate(reasons)]

        _output_add_unchanged(skips, verbose=False)

        assert capsys.readouterr().out == summary


class TestCatalogIndexAfterAdd:
    """The catalog-level versions.json matches each collection after one add."""

    @pytest.mark.integration
    def test_the_index_counts_the_side_step_assets(
        self,
        run_cli: RunCli,
        conformant_catalog: Path,
        valid_rgb_cog: Path,
        valid_points_parquet: Path,
    ) -> None:
        """The item mirror and the thumbnails are in both files, and a no-op re-add writes neither."""
        root = conformant_catalog
        (root / "imagery" / "scene").mkdir(parents=True)
        (root / "imagery" / "scene" / "scene.tif").write_bytes(valid_rgb_cog.read_bytes())
        (root / "roads").mkdir()
        (root / "roads" / "roads.parquet").write_bytes(valid_points_parquet.read_bytes())

        args = ["add", "--portolan-dir", str(root), str(root)]
        run_cli(*args)
        after_first = (root / "versions.json").read_bytes()
        run_cli(*args)

        # A second add changes nothing, so it leaves the index bytes alone.
        assert (root / "versions.json").read_bytes() == after_first
        index = json.loads(after_first)["collections"]
        for collection_id in ("imagery", "roads"):
            versions = json.loads(
                (root / collection_id / "versions.json").read_text(encoding="utf-8")
            )
            current = next(
                v for v in versions["versions"] if v["version"] == versions["current_version"]
            )
            entry = index[collection_id]
            assert (entry["current_version"], entry["asset_count"], entry["total_size_bytes"]) == (
                versions["current_version"],
                len(current["assets"]),
                sum(asset["size_bytes"] for asset in current["assets"].values()),
            ), collection_id


class TestSideStepExitAfterAdd:
    """A side step that fails after ``add_files`` still commits the added files.

    ``add_files`` writes collection.json for each collection. The mirror, the
    PMTiles, and the thumbnail steps run after it. A failure in one of them
    keeps its exit code, and versions.json and the catalog index still record
    the files that collection.json lists.
    """

    @staticmethod
    def _assert_catalog_agrees(root: Path, collection_ids: list[str]) -> None:
        from portolan_cli.sync.checksums import compute_checksum

        index = json.loads((root / "versions.json").read_text(encoding="utf-8"))["collections"]
        for collection_id in collection_ids:
            collection_dir = root / collection_id
            versions = json.loads((collection_dir / "versions.json").read_text(encoding="utf-8"))
            assert versions["current_version"] == "1.0.0", collection_id
            records = versions["versions"][-1]["assets"]
            stac = json.loads((collection_dir / "collection.json").read_text(encoding="utf-8"))
            for asset in stac["assets"].values():
                key = asset["href"].removeprefix("./")
                assert records[key]["sha256"] == compute_checksum(collection_dir / key), key
            entry = index[collection_id]
            assert (entry["current_version"], entry["asset_count"], entry["total_size_bytes"]) == (
                "1.0.0",
                len(records),
                sum(record["size_bytes"] for record in records.values()),
            ), collection_id

    @pytest.mark.integration
    def test_pmtiles_without_tippecanoe(
        self,
        run_cli: RunCli,
        conformant_catalog: Path,
        valid_points_parquet: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``--pmtiles`` without tippecanoe exits 1 and keeps the version of the add."""
        import portolan_cli.viz.pmtiles as pmtiles

        def exit_like_missing_tippecanoe(*_args: object, **_kwargs: object) -> None:
            raise SystemExit(1)

        monkeypatch.setattr(pmtiles, "generate_or_suggest_pmtiles", exit_like_missing_tippecanoe)
        root = conformant_catalog
        (root / "roads").mkdir()
        (root / "roads" / "roads.parquet").write_bytes(valid_points_parquet.read_bytes())

        run_cli("add", "--pmtiles", "--portolan-dir", str(root), str(root), expect=1)

        self._assert_catalog_agrees(root, ["roads"])

    @pytest.mark.integration
    def test_an_explicit_mirror_failure(
        self,
        run_cli: RunCli,
        conformant_catalog: Path,
        valid_rgb_cog: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A ``--stac-geoparquet`` failure still raises and keeps the version of the add."""
        import portolan_cli.stac_parquet as stac_parquet

        def broken_mirror(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("mirror broke")

        monkeypatch.setattr(stac_parquet, "generate_items_parquet", broken_mirror)
        root = conformant_catalog
        (root / "imagery" / "scene").mkdir(parents=True)
        (root / "imagery" / "scene" / "scene.tif").write_bytes(valid_rgb_cog.read_bytes())

        with pytest.raises(RuntimeError, match="mirror broke"):
            run_cli("add", "--stac-geoparquet", "--portolan-dir", str(root), str(root))

        assert not (root / "imagery" / "items.parquet").exists()
        self._assert_catalog_agrees(root, ["imagery"])

    @pytest.mark.integration
    def test_a_thumbnail_exception(
        self,
        run_cli: RunCli,
        conformant_catalog: Path,
        valid_points_parquet: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An exception in the thumbnail step still raises and keeps the version of the add."""
        import portolan_cli.collection_thumbnail as collection_thumbnail

        def broken_thumbnails(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("thumbnail broke")

        monkeypatch.setattr(collection_thumbnail, "ensure_collection_thumbnails", broken_thumbnails)
        root = conformant_catalog
        (root / "roads").mkdir()
        (root / "roads" / "roads.parquet").write_bytes(valid_points_parquet.read_bytes())

        with pytest.raises(RuntimeError, match="thumbnail broke"):
            run_cli("add", "--portolan-dir", str(root), str(root))

        self._assert_catalog_agrees(root, ["roads"])


class TestAddUnknownHandlerPin:
    """A ``conversion.handlers`` pin to a missing handler fails add with a typed error."""

    @pytest.mark.integration
    @pytest.mark.parametrize("json_mode", [False, True], ids=["text", "json"])
    def test_the_error_names_the_pin_and_the_known_handlers(
        self,
        run_cli: RunCli,
        conformant_catalog: Path,
        valid_points_geojson: Path,
        json_mode: bool,
    ) -> None:
        root = conformant_catalog
        config = root / ".portolan" / "config.yaml"
        config.write_text(config.read_text() + "conversion:\n  handlers:\n    geojson: nosuch\n")
        (root / "pts").mkdir()
        points = root / "pts" / "points.geojson"
        points.write_bytes(valid_points_geojson.read_bytes())
        prefix = ["--format", "json"] if json_mode else []

        result = run_cli(*prefix, "add", "--portolan-dir", str(root), str(points), expect=1)

        if json_mode:
            errors = json.loads(result.stdout)["errors"]
            assert [e["type"] for e in errors] == ["UnknownFormatHandlerError"]
            message = errors[0]["message"]
        else:
            message = result.stderr
        assert "conversion.handlers pins 'nosuch' for .geojson" in message
        assert "Available: " in message
        assert "geoparquet" in message.split("Available: ")[1]
        assert not (root / "pts" / "collection.json").exists()


class TestRmJsonOutput:
    """Tests for rm command JSON output mode."""

    @pytest.mark.unit
    def test_rm_json_output(self, runner: CliRunner) -> None:
        """rm --format json outputs valid JSON envelope."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "test.parquet"
            test_file.write_bytes(b"data")

            with patch("portolan_cli.cli.remove_files") as mock_rm:
                mock_rm.return_value = ([test_file], [])  # (removed, skipped)

                result = runner.invoke(
                    cli,
                    ["--format", "json", "rm", "--force", str(test_file)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                envelope = json.loads(result.output)
                assert envelope["success"] is True
                assert envelope["command"] == "rm"
                assert "dry_run" in envelope["data"]

    @pytest.mark.unit
    def test_rm_json_dry_run(self, runner: CliRunner) -> None:
        """rm --format json --dry-run outputs preview without requiring force."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "test.parquet"
            test_file.write_bytes(b"data")

            with patch("portolan_cli.cli.remove_files") as mock_rm:
                mock_rm.return_value = ([test_file], [])

                result = runner.invoke(
                    cli,
                    ["--format", "json", "rm", "--dry-run", str(test_file)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                envelope = json.loads(result.output)
                assert envelope["success"] is True
                assert envelope["data"]["dry_run"] is True


class TestCheckPartitionPrompt:
    """Tests for _check_partition_prompt helper function."""

    @pytest.mark.unit
    def test_returns_false_when_partitioning_disabled(self, tmp_path: Path) -> None:
        """When partitioning.enabled is False, should return False (no skip)."""
        from portolan_cli.cli import _check_partition_prompt

        setup_catalog(tmp_path)

        # Create config with partitioning disabled
        config_path = tmp_path / ".portolan" / "config.yaml"
        config_path.write_text("partitioning:\n  enabled: false\n")

        # Create a "large" parquet file (mocked size)
        parquet_file = tmp_path / "data.parquet"
        parquet_file.write_bytes(b"x" * 100)

        result = _check_partition_prompt([parquet_file], tmp_path)

        assert result is False

    @pytest.mark.unit
    def test_returns_false_when_no_large_files(self, tmp_path: Path) -> None:
        """When no files exceed threshold, should return False."""
        from portolan_cli.cli import _check_partition_prompt

        setup_catalog(tmp_path)

        # Create a small parquet file
        parquet_file = tmp_path / "small.parquet"
        parquet_file.write_bytes(b"x" * 100)

        result = _check_partition_prompt([parquet_file], tmp_path)

        assert result is False

    @pytest.mark.unit
    def test_returns_false_when_not_tty(self, tmp_path: Path) -> None:
        """When not running in TTY, should return False (non-interactive)."""
        from portolan_cli.cli import _check_partition_prompt

        setup_catalog(tmp_path)

        parquet_file = tmp_path / "data.parquet"
        parquet_file.write_bytes(b"x" * 100)

        # Mock should_partition to return True (imported inside function from partitioning module)
        with patch("portolan_cli.partitioning.should_partition", return_value=True):
            # sys.stderr.isatty() returns False in test environment
            result = _check_partition_prompt([parquet_file], tmp_path)

        assert result is False

    @pytest.mark.unit
    def test_returns_false_when_prompt_disabled(self, tmp_path: Path) -> None:
        """When partitioning.prompt is False, should not prompt."""
        from portolan_cli.cli import _check_partition_prompt

        setup_catalog(tmp_path)

        # Create config with prompt disabled
        config_path = tmp_path / ".portolan" / "config.yaml"
        config_path.write_text("partitioning:\n  enabled: true\n  prompt: false\n")

        parquet_file = tmp_path / "data.parquet"
        parquet_file.write_bytes(b"x" * 100)

        with patch("portolan_cli.partitioning.should_partition", return_value=True):
            result = _check_partition_prompt([parquet_file], tmp_path)

        assert result is False

    @pytest.mark.unit
    def test_scans_directories_recursively(self, tmp_path: Path) -> None:
        """Should scan directories for parquet files."""
        from portolan_cli.cli import _check_partition_prompt

        setup_catalog(tmp_path)

        # Create nested parquet file
        subdir = tmp_path / "data" / "nested"
        subdir.mkdir(parents=True)
        parquet_file = subdir / "deep.parquet"
        parquet_file.write_bytes(b"x" * 100)

        # Pass directory, not file
        result = _check_partition_prompt([tmp_path / "data"], tmp_path)

        # Should return False (no TTY, no large files without mock)
        assert result is False
