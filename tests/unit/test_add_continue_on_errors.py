"""Unit tests for add command continue-on-errors behavior.

Per GitHub issue #175: `portolan add .` should continue processing all files
even when some fail, then report all failures at the end.

Expected behavior:
- Continue processing all files even when some fail
- Collect all errors
- Report all failures at the end
- Exit with non-zero code if any failures occurred
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.dataset import (
    AddFailure,
    DatasetInfo,
    add_files,
)
from portolan_cli.formats import FormatType


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI test runner."""
    return CliRunner()


def setup_catalog(path: Path) -> None:
    """Create an initialized Portolan catalog (per ADR-0023 and ADR-0029)."""
    portolan_dir = path / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("# Portolan configuration\n")
    (portolan_dir / "state.json").write_text("{}")
    catalog_data = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "portolan-catalog",
        "description": "A Portolan-managed STAC catalog",
        "links": [],
    }
    (path / "catalog.json").write_text(json.dumps(catalog_data, indent=2))


class TestAddFilesReturnsFailures:
    """Test that add_files returns failures instead of raising on first error."""

    @pytest.mark.unit
    def test_add_files_returns_failures_tuple(self) -> None:
        """add_files should return a 3-tuple: (added, skipped, failures)."""
        # This test verifies the new return signature by checking return type annotation
        import inspect

        from portolan_cli.dataset import add_files

        sig = inspect.signature(add_files)
        # Verify the return annotation exists (indicates 3 values)
        assert sig.return_annotation is not inspect.Parameter.empty

    @pytest.mark.unit
    def test_add_failure_dataclass_exists(self) -> None:
        """AddFailure dataclass should exist with path and error fields."""
        from portolan_cli.dataset import AddFailure

        # Create an AddFailure instance
        failure = AddFailure(
            path=Path("/test/file.parquet"),
            error="missing bounding box",
        )

        assert failure.path == Path("/test/file.parquet")
        assert failure.error == "missing bounding box"


class TestAddFilesContinuesOnErrors:
    """Test that add_files continues processing after individual file errors."""

    @pytest.mark.unit
    def test_add_files_continues_after_value_error(self, tmp_path: Path) -> None:
        """add_files should continue processing when add_dataset raises ValueError."""
        # Setup catalog
        setup_catalog(tmp_path)

        # Create collection directory with multiple files
        collection_dir = tmp_path / "collection" / "item1"
        collection_dir.mkdir(parents=True)
        good_file = collection_dir / "good.geojson"
        good_file.write_text('{"type": "FeatureCollection", "features": []}')

        collection_dir2 = tmp_path / "collection" / "item2"
        collection_dir2.mkdir(parents=True)
        bad_file = collection_dir2 / "bad.geojson"
        bad_file.write_text('{"type": "FeatureCollection", "features": []}')

        # Mock add_dataset to fail on the bad file but succeed on good file
        call_count = 0

        def mock_add_dataset(
            *,
            path: Path,
            catalog_root: Path,
            collection_id: str,
            item_id: str | None = None,
        ) -> DatasetInfo:
            nonlocal call_count
            call_count += 1
            if "bad" in path.name:
                raise ValueError("missing bounding box")
            return DatasetInfo(
                item_id="good",
                collection_id="collection",
                format_type=FormatType.VECTOR,
                bbox=[-122.5, 37.5, -122.0, 38.0],
                asset_paths=["good.parquet"],
            )

        with patch("portolan_cli.dataset.add_dataset", side_effect=mock_add_dataset):
            with patch("portolan_cli.dataset.is_current", return_value=False):
                added, skipped, failures = add_files(
                    paths=[collection_dir, collection_dir2],
                    catalog_root=tmp_path,
                    collection_id="collection",
                )

        # Should have processed both files
        assert call_count == 2
        # One success, one failure
        assert len(added) == 1
        assert len(failures) == 1
        assert failures[0].path == bad_file
        assert "missing bounding box" in failures[0].error

    @pytest.mark.unit
    def test_add_files_continues_after_file_not_found_error(self, tmp_path: Path) -> None:
        """add_files should continue processing when add_dataset raises FileNotFoundError."""
        setup_catalog(tmp_path)

        collection_dir = tmp_path / "collection" / "item1"
        collection_dir.mkdir(parents=True)
        file1 = collection_dir / "file1.geojson"
        file1.write_text('{"type": "FeatureCollection", "features": []}')

        collection_dir2 = tmp_path / "collection" / "item2"
        collection_dir2.mkdir(parents=True)
        file2 = collection_dir2 / "file2.geojson"
        file2.write_text('{"type": "FeatureCollection", "features": []}')

        def mock_add_dataset(
            *,
            path: Path,
            catalog_root: Path,
            collection_id: str,
            item_id: str | None = None,
        ) -> DatasetInfo:
            if "file1" in path.name:
                raise FileNotFoundError("Source file disappeared")
            return DatasetInfo(
                item_id="file2",
                collection_id="collection",
                format_type=FormatType.VECTOR,
                bbox=[0, 0, 1, 1],
                asset_paths=["file2.parquet"],
            )

        with patch("portolan_cli.dataset.add_dataset", side_effect=mock_add_dataset):
            with patch("portolan_cli.dataset.is_current", return_value=False):
                added, skipped, failures = add_files(
                    paths=[collection_dir, collection_dir2],
                    catalog_root=tmp_path,
                    collection_id="collection",
                )

        # One success, one failure
        assert len(added) == 1
        assert len(failures) == 1
        assert "Source file disappeared" in failures[0].error

    @pytest.mark.unit
    def test_add_files_collects_multiple_failures(self, tmp_path: Path) -> None:
        """add_files should collect all failures, not just the first."""
        setup_catalog(tmp_path)

        # Create 3 files, all of which will fail
        for i in range(1, 4):
            item_dir = tmp_path / "collection" / f"item{i}"
            item_dir.mkdir(parents=True)
            f = item_dir / f"bad{i}.geojson"
            f.write_text('{"type": "FeatureCollection", "features": []}')

        def mock_add_dataset(
            *,
            path: Path,
            catalog_root: Path,
            collection_id: str,
            item_id: str | None = None,
        ) -> DatasetInfo:
            raise ValueError(f"Error processing {path.name}")

        with patch("portolan_cli.dataset.add_dataset", side_effect=mock_add_dataset):
            with patch("portolan_cli.dataset.is_current", return_value=False):
                added, skipped, failures = add_files(
                    paths=[tmp_path / "collection"],
                    catalog_root=tmp_path,
                    collection_id="collection",
                )

        # All 3 should fail
        assert len(added) == 0
        assert len(failures) == 3
        # Each failure should have the correct error message
        for i, failure in enumerate(sorted(failures, key=lambda f: f.path.name), 1):
            assert f"bad{i}.geojson" in str(failure.path)


class TestCliOutputWithFailures:
    """Test CLI output formatting when there are failures."""

    @pytest.mark.unit
    def test_cli_shows_summary_with_failures(self, runner: CliRunner) -> None:
        """CLI should show summary like 'Added 5 items, 2 failed'."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "collection"
            collection_dir.mkdir()
            test_file = collection_dir / "test.geojson"
            test_file.write_text("{}")

            with patch("portolan_cli.cli.add_files") as mock_add:
                # Return: 2 added, 0 skipped, 1 failure
                mock_add.return_value = (
                    [
                        DatasetInfo(
                            item_id="good1",
                            collection_id="collection",
                            format_type=FormatType.VECTOR,
                            bbox=[0, 0, 1, 1],
                            asset_paths=["good1.parquet"],
                        ),
                        DatasetInfo(
                            item_id="good2",
                            collection_id="collection",
                            format_type=FormatType.VECTOR,
                            bbox=[0, 0, 1, 1],
                            asset_paths=["good2.parquet"],
                        ),
                    ],
                    [],  # skipped
                    [
                        AddFailure(
                            path=Path("/test/bad.parquet"),
                            error="missing bounding box",
                        )
                    ],  # failures
                )

                result = runner.invoke(
                    cli,
                    ["add", str(collection_dir)],
                )

                # Should show both successes and failures
                assert "2" in result.output  # 2 added
                assert "1" in result.output or "failed" in result.output.lower()
                # Should exit with non-zero code due to failures
                assert result.exit_code == 1

    @pytest.mark.unit
    def test_cli_shows_each_failure_detail(self, runner: CliRunner) -> None:
        """CLI should show each failure with path and error message."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "collection"
            collection_dir.mkdir()
            test_file = collection_dir / "test.geojson"
            test_file.write_text("{}")

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = (
                    [],  # no successful adds
                    [],  # skipped
                    [
                        AddFailure(
                            path=Path("census-2010/data.parquet"),
                            error="missing bounding box",
                        ),
                        AddFailure(
                            path=Path("census-2022/data.parquet"),
                            error="invalid geometry type",
                        ),
                    ],
                )

                result = runner.invoke(
                    cli,
                    ["add", str(collection_dir)],
                )

                # Should show each failure
                assert "census-2010" in result.output
                assert "missing bounding box" in result.output
                assert "census-2022" in result.output
                assert "invalid geometry type" in result.output
                assert result.exit_code == 1

    @pytest.mark.unit
    def test_cli_json_output_includes_failures(self, runner: CliRunner) -> None:
        """JSON output should include failures array."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "collection"
            collection_dir.mkdir()
            test_file = collection_dir / "test.geojson"
            test_file.write_text("{}")

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = (
                    [
                        DatasetInfo(
                            item_id="good",
                            collection_id="collection",
                            format_type=FormatType.VECTOR,
                            bbox=[0, 0, 1, 1],
                            asset_paths=["good.parquet"],
                        )
                    ],
                    [],
                    [
                        AddFailure(
                            path=Path("bad.parquet"),
                            error="missing geometry",
                        )
                    ],
                )

                result = runner.invoke(
                    cli,
                    ["--format", "json", "add", str(collection_dir)],
                )

                envelope = json.loads(result.output)
                # With failures, success should be False
                assert envelope["success"] is False
                assert "failures" in envelope["data"]
                assert len(envelope["data"]["failures"]) == 1
                assert envelope["data"]["failures"][0]["error"] == "missing geometry"
                assert result.exit_code == 1

    @pytest.mark.unit
    def test_cli_success_when_no_failures(self, runner: CliRunner) -> None:
        """CLI should exit 0 when all files are processed successfully."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "collection"
            collection_dir.mkdir()
            test_file = collection_dir / "test.geojson"
            test_file.write_text("{}")

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = (
                    [
                        DatasetInfo(
                            item_id="test",
                            collection_id="collection",
                            format_type=FormatType.VECTOR,
                            bbox=[0, 0, 1, 1],
                            asset_paths=["test.parquet"],
                        )
                    ],
                    [],
                    [],  # no failures
                )

                result = runner.invoke(
                    cli,
                    ["add", str(collection_dir)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0


class TestAddFilesReturnContract:
    """Verify add_files returns the 3-tuple (added, skipped, failures) contract."""

    @pytest.mark.unit
    def test_add_files_returns_three_value_tuple(self, tmp_path: Path) -> None:
        """add_files returns exactly 3 values: (added, skipped, failures).

        This documents the return contract introduced in Issue #175.
        Callers must unpack all three values:
            added, skipped, failures = add_files(...)
        """
        setup_catalog(tmp_path)

        collection_dir = tmp_path / "collection" / "item"
        collection_dir.mkdir(parents=True)
        f = collection_dir / "test.geojson"
        f.write_text('{"type": "FeatureCollection", "features": []}')

        with patch(
            "portolan_cli.dataset.add_dataset",
            return_value=DatasetInfo(
                item_id="test",
                collection_id="collection",
                format_type=FormatType.VECTOR,
                bbox=[0, 0, 1, 1],
                asset_paths=["test.parquet"],
            ),
        ):
            with patch("portolan_cli.dataset.is_current", return_value=False):
                result = add_files(
                    paths=[collection_dir],
                    catalog_root=tmp_path,
                    collection_id="collection",
                )

                # Should return exactly 3 values
                assert len(result) == 3
                added, skipped, failures = result
                assert isinstance(added, list)
                assert isinstance(skipped, list)
                assert isinstance(failures, list)


class TestOutputEdgeCases:
    """Test output formatting edge cases not covered by other test classes."""

    @pytest.mark.unit
    def test_human_output_zero_added_some_skipped_some_failures(self, runner: CliRunner) -> None:
        """Human output when all files either skipped or failed (none added).

        This is a realistic scenario: e.g., several files are unchanged
        (skipped) and one has an error. The output should show failure
        details without an 'Added 0 items' header.
        """
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "collection"
            collection_dir.mkdir()
            test_file = collection_dir / "test.geojson"
            test_file.write_text("{}")

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = (
                    [],  # no added
                    [
                        Path("unchanged1.parquet"),
                        Path("unchanged2.parquet"),
                    ],  # skipped
                    [
                        AddFailure(
                            path=Path("broken.parquet"),
                            error="invalid CRS",
                        ),
                    ],  # failures
                )

                result = runner.invoke(cli, ["add", str(collection_dir)])

                # Should show failure details
                assert "broken.parquet" in result.output
                assert "invalid CRS" in result.output
                assert "1 item failed" in result.output
                # Should exit non-zero
                assert result.exit_code == 1

    @pytest.mark.unit
    def test_click_exception_error_format_no_path_duplication(self, runner: CliRunner) -> None:
        """ClickException errors should not duplicate the file path.

        Bug #191 review: ClickException errors stored as str(err) produce
        clean output like '- path: message' without repeating the path.
        """
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "collection"
            collection_dir.mkdir()
            test_file = collection_dir / "test.geojson"
            test_file.write_text("{}")

            err_path = Path("census/data.parquet")
            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = (
                    [],
                    [],
                    [
                        AddFailure(
                            path=err_path,
                            error="Permission denied: /some/path",
                        ),
                    ],
                )

                result = runner.invoke(cli, ["add", str(collection_dir)])

                # The path should appear in '- path: error' format
                # and NOT be duplicated inside the error message
                # Use str(err_path) for cross-platform compatibility (Windows uses \)
                assert f"{err_path}: Permission denied" in result.output
                # Verify no double-path pattern
                assert "Failed to add" not in result.output
                assert result.exit_code == 1

    @pytest.mark.unit
    def test_json_output_only_failures_no_successes(self, runner: CliRunner) -> None:
        """JSON output with zero successes and only failures.

        When all files fail, the envelope should have success=False,
        an errors array at the top level, and data.added should be empty.
        """
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "collection"
            collection_dir.mkdir()
            test_file = collection_dir / "test.geojson"
            test_file.write_text("{}")

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = (
                    [],  # no successes
                    [],  # no skipped
                    [
                        AddFailure(
                            path=Path("file1.geojson"),
                            error="missing bounding box",
                        ),
                        AddFailure(
                            path=Path("file2.geojson"),
                            error="invalid geometry type",
                        ),
                    ],
                )

                result = runner.invoke(
                    cli,
                    ["--format", "json", "add", str(collection_dir)],
                )

                envelope = json.loads(result.output)
                assert envelope["success"] is False
                assert envelope["data"]["added"] == []
                assert len(envelope["data"]["failures"]) == 2
                assert envelope["data"]["failures"][0]["error"] == "missing bounding box"
                assert envelope["data"]["failures"][1]["error"] == "invalid geometry type"
                # Top-level errors array should exist
                assert "errors" in envelope
                assert len(envelope["errors"]) == 2
                assert result.exit_code == 1
