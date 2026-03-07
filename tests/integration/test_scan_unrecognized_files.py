"""Integration tests for listing unrecognized files in scan output.

Tests for GitHub issue #181: Make scan list specific unrecognized files, not just count.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    """Create a Click test runner."""
    return CliRunner()


@pytest.mark.integration
class TestScanUnrecognizedFilesOutput:
    """Integration tests for scan command output with unrecognized files."""

    def test_scan_lists_unrecognized_files_in_output(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Scan output lists specific unrecognized files, not just a count.

        GitHub issue #181: Instead of "5 files with unrecognized format",
        should show:
        "5 files with unrecognized format:
          - legacy.xlsx
          - notes.docx
          ..."
        """
        # Create a directory with known and unknown files
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create a valid geojson file
        geojson_file = data_dir / "points.geojson"
        geojson_file.write_text('{"type":"FeatureCollection","features":[]}')

        # Create unrecognized files (truly unknown extensions)
        unknown_files = [
            "data.xyz",  # Unknown extension
            "notes.abc",  # Unknown extension
            "config.def",  # Unknown extension
            "legacy.ghi",  # Unknown extension
        ]
        for filename in unknown_files:
            (data_dir / filename).write_text("test content")

        # Run scan
        result = runner.invoke(cli, ["scan", str(data_dir)])

        # Should exit with 0 (warnings don't cause failure)
        assert result.exit_code == 0

        # Output should list the specific unrecognized files
        output = result.output.lower()
        assert "unrecognized format" in output

        # Check that specific filenames appear in output
        for filename in unknown_files:
            assert filename.lower() in output, (
                f"Expected '{filename}' to appear in scan output:\n{result.output}"
            )

    def test_scan_unrecognized_files_with_paths(self, runner: CliRunner, tmp_path: Path) -> None:
        """Unrecognized files should be shown with relative paths."""
        data_dir = tmp_path / "data"
        (data_dir / "subdir").mkdir(parents=True)

        # Create unrecognized files in nested directories
        (data_dir / "file1.xyz").write_text("test")
        (data_dir / "subdir" / "file2.xyz").write_text("test")

        result = runner.invoke(cli, ["scan", str(data_dir)])
        assert result.exit_code == 0

        # Both files should appear in output
        output = result.output.lower()
        assert "file1.xyz" in output
        assert "file2.xyz" in output

    def test_scan_many_unrecognized_files_truncated_by_default(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """When many unrecognized files exist, should truncate after 10.

        Verifies:
        - First 10 files are shown (sorted alphabetically)
        - Files 11+ are NOT shown
        - Truncation message shows exact count remaining
        """
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create 15 unrecognized files
        for i in range(15):
            (data_dir / f"unknown_{i:02d}.xyz").write_text("test")

        result = runner.invoke(cli, ["scan", str(data_dir)])
        assert result.exit_code == 0

        output = result.output
        # Should indicate 15 files with unrecognized format
        assert "15 files with unrecognized format" in output.lower()

        # First 10 files should be shown (sorted: unknown_00 through unknown_09)
        for i in range(10):
            assert f"unknown_{i:02d}.xyz" in output, (
                f"Expected unknown_{i:02d}.xyz in truncated output"
            )

        # Files 10-14 should NOT be shown
        for i in range(10, 15):
            assert f"unknown_{i:02d}.xyz" not in output, (
                f"Expected unknown_{i:02d}.xyz to be truncated"
            )

        # Should show exact truncation count: "and 5 more"
        assert "5 more" in output.lower()
        assert "--all" in output

    def test_scan_all_flag_shows_complete_unrecognized_list(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """--all flag shows complete list of unrecognized files.

        Parallel to how --all shows all issues.
        """
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create many unrecognized files
        unknown_files = [f"unknown_{i:02d}.xyz" for i in range(12)]
        for filename in unknown_files:
            (data_dir / filename).write_text("test")

        result = runner.invoke(cli, ["scan", str(data_dir), "--all"])
        assert result.exit_code == 0

        output = result.output.lower()
        # All files should appear when using --all
        for filename in unknown_files:
            assert filename.lower() in output, (
                f"Expected '{filename}' in --all output:\n{result.output}"
            )

    def test_scan_no_unrecognized_files_no_warning(self, runner: CliRunner, tmp_path: Path) -> None:
        """When all skipped files are recognized, no unrecognized warning.

        Only catalog metadata, docs, etc. (no actual unknown files).
        """
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create only recognized non-geospatial files
        (data_dir / "README.md").write_text("# Data")
        (data_dir / "catalog.json").write_text("{}")
        (data_dir / "data.csv").write_text("a,b,c\n1,2,3")

        result = runner.invoke(cli, ["scan", str(data_dir)])
        assert result.exit_code == 0

        # Should NOT have unrecognized warning
        assert "unrecognized format" not in result.output.lower()

    def test_scan_mixed_recognized_and_unrecognized_files(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """When mixing recognized and unknown files, list both clearly."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create a valid geojson
        (data_dir / "data.geojson").write_text('{"type":"FeatureCollection","features":[]}')

        # Create recognized non-geospatial files
        (data_dir / "README.md").write_text("# Data")
        (data_dir / "notes.csv").write_text("a,b\n1,2")

        # Create unrecognized files
        (data_dir / "unknown1.xyz").write_text("?")
        (data_dir / "unknown2.abc").write_text("?")

        result = runner.invoke(cli, ["scan", str(data_dir)])
        assert result.exit_code == 0

        output = result.output.lower()
        # Should mention unrecognized format
        assert "unrecognized format" in output
        # Should list the unknown files
        assert "unknown1.xyz" in output
        assert "unknown2.abc" in output
        # Should also mention recognized files
        assert "readme" in output or "csv" in output or "documentation" in output

    def test_scan_output_format_has_indentation(self, runner: CliRunner, tmp_path: Path) -> None:
        """Unrecognized files should be indented like other scan output.

        Consistent with how issues are indented in the output.
        """
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        (data_dir / "file1.xyz").write_text("test")
        (data_dir / "file2.xyz").write_text("test")

        result = runner.invoke(cli, ["scan", str(data_dir)])
        assert result.exit_code == 0

        output_lines = result.output.split("\n")
        # Find the unrecognized files section
        unrecognized_idx = None
        for i, line in enumerate(output_lines):
            if "unrecognized" in line.lower():
                unrecognized_idx = i
                break

        assert unrecognized_idx is not None, "No unrecognized line found"

        # Files after the warning should be indented (start with whitespace)
        for line in output_lines[unrecognized_idx + 1 :]:
            if line.strip() and ("file1" in line or "file2" in line):
                # Should be indented
                assert line[0] in (" ", "\t"), f"File list should be indented:\n{line}"
                break

    def test_scan_json_output_includes_unrecognized_files(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """JSON output should include full list of unrecognized files."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        (data_dir / "file1.xyz").write_text("test")
        (data_dir / "file2.xyz").write_text("test")

        result = runner.invoke(cli, ["scan", str(data_dir), "--json"])
        assert result.exit_code == 0

        import json

        output_json = json.loads(result.output)
        # Should have data.skipped in JSON structure
        assert "data" in output_json
        data = output_json.get("data", {})
        assert "skipped" in data
        # Unrecognized files should be in skipped
        skipped_files = data.get("skipped", [])
        assert len(skipped_files) > 0

        # Find unknown files using explicit enum value check
        from portolan_cli.scan_classify import SkipReasonType

        unknown_files = [
            f for f in skipped_files if f.get("reason_type") == SkipReasonType.UNKNOWN_FORMAT.value
        ]
        assert len(unknown_files) == 2, (
            f"Expected 2 unknown files in JSON, got {len(unknown_files)}: {unknown_files}"
        )

    def test_scan_exactly_10_unrecognized_files_no_truncation(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Boundary test: exactly 10 files should all be shown without truncation."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create exactly 10 unrecognized files
        for i in range(10):
            (data_dir / f"file_{i:02d}.xyz").write_text("test")

        result = runner.invoke(cli, ["scan", str(data_dir)])
        assert result.exit_code == 0

        output = result.output
        # All 10 files should be shown
        for i in range(10):
            assert f"file_{i:02d}.xyz" in output, (
                f"Expected file_{i:02d}.xyz in output for exactly 10 files"
            )

        # No truncation message
        assert "more" not in output.lower()
        assert "--all" not in output

    def test_scan_exactly_11_unrecognized_files_truncates_to_10(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Boundary test: exactly 11 files should truncate, showing 10 + '1 more'."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create exactly 11 unrecognized files
        for i in range(11):
            (data_dir / f"file_{i:02d}.xyz").write_text("test")

        result = runner.invoke(cli, ["scan", str(data_dir)])
        assert result.exit_code == 0

        output = result.output
        # First 10 files should be shown
        for i in range(10):
            assert f"file_{i:02d}.xyz" in output, f"Expected file_{i:02d}.xyz in output"

        # 11th file should NOT be shown
        assert "file_10.xyz" not in output

        # Truncation message should show exactly "1 more"
        assert "1 more" in output.lower()
        assert "--all" in output

    def test_scan_only_unrecognized_files_no_geo_assets(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Directory with only unknown files should list them all (up to 10)."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create only unrecognized files, no geo-assets
        for i in range(5):
            (data_dir / f"mystery_{i}.unk").write_text("data")

        result = runner.invoke(cli, ["scan", str(data_dir)])
        assert result.exit_code == 0

        output = result.output.lower()
        # Should show no geo-assets message
        assert "no geo-asset" in output or "0 geo-asset" in output

        # Should still list all unrecognized files
        assert "unrecognized format" in output
        for i in range(5):
            assert f"mystery_{i}.unk" in output

    def test_scan_filenames_with_spaces(self, runner: CliRunner, tmp_path: Path) -> None:
        """Filenames with spaces should be handled correctly."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create files with spaces in names
        (data_dir / "my file.xyz").write_text("test")
        (data_dir / "another document.abc").write_text("test")

        result = runner.invoke(cli, ["scan", str(data_dir)])
        assert result.exit_code == 0

        output = result.output
        assert "my file.xyz" in output
        assert "another document.abc" in output

    def test_scan_filenames_with_unicode(self, runner: CliRunner, tmp_path: Path) -> None:
        """Filenames with unicode characters should be handled correctly."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create files with unicode characters
        (data_dir / "données.xyz").write_text("test")
        (data_dir / "地図データ.abc").write_text("test")
        (data_dir / "файл.def").write_text("test")

        result = runner.invoke(cli, ["scan", str(data_dir)])
        assert result.exit_code == 0

        output = result.output
        assert "données.xyz" in output
        assert "地図データ.abc" in output
        assert "файл.def" in output

    def test_scan_filenames_with_special_characters(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Filenames with special characters should be handled correctly."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create files with special characters (valid on most filesystems)
        special_files = [
            "file-with-dashes.xyz",
            "file_with_underscores.xyz",
            "file.multiple.dots.xyz",
            "file(with)parens.xyz",
            "file[with]brackets.xyz",
            "file@symbol.xyz",
            "file#hash.xyz",
            "file%percent.xyz",
        ]
        for filename in special_files:
            (data_dir / filename).write_text("test")

        result = runner.invoke(cli, ["scan", str(data_dir)])
        assert result.exit_code == 0

        output = result.output
        for filename in special_files:
            assert filename in output, f"Expected '{filename}' in output"

    def test_scan_very_long_filenames(self, runner: CliRunner, tmp_path: Path) -> None:
        """Very long filenames (>100 chars) should be handled correctly."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create files with long names (stay under filesystem limits ~255)
        long_name_1 = "a" * 110 + ".xyz"  # 114 chars total
        long_name_2 = "b" * 150 + ".abc"  # 154 chars total
        (data_dir / long_name_1).write_text("test")
        (data_dir / long_name_2).write_text("test")

        result = runner.invoke(cli, ["scan", str(data_dir)])
        assert result.exit_code == 0

        output = result.output
        # Long filenames should appear in output (possibly truncated for display)
        # At minimum, the extension should be visible
        assert ".xyz" in output
        assert ".abc" in output
        # The full filename should be present since we don't truncate in listing
        assert long_name_1 in output
        assert long_name_2 in output

    def test_scan_json_explicit_enum_values(self, runner: CliRunner, tmp_path: Path) -> None:
        """JSON output uses exact enum values, not substring matches."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        (data_dir / "unknown.xyz").write_text("test")
        (data_dir / "readme.md").write_text("# Docs")
        (data_dir / "data.csv").write_text("a,b\n1,2")

        result = runner.invoke(cli, ["scan", str(data_dir), "--json"])
        assert result.exit_code == 0

        import json

        from portolan_cli.scan_classify import FileCategory, SkipReasonType

        output_json = json.loads(result.output)
        skipped_files = output_json.get("data", {}).get("skipped", [])

        # Check that reason_type values match exact enum values
        reason_types = {f["reason_type"] for f in skipped_files}

        # Verify using exact enum values
        assert SkipReasonType.UNKNOWN_FORMAT.value in reason_types
        assert SkipReasonType.NOT_GEOSPATIAL.value in reason_types

        # Check category values are exact enum values
        categories = {f["category"] for f in skipped_files}
        assert FileCategory.UNKNOWN.value in categories
        assert FileCategory.DOCUMENTATION.value in categories
        assert FileCategory.TABULAR_DATA.value in categories
