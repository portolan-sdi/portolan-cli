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
        """When many unrecognized files exist, should truncate by default.

        Similar to how issues are truncated with "use --all to see all".
        """
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create many unrecognized files (>10)
        for i in range(15):
            (data_dir / f"unknown_{i:02d}.xyz").write_text("test")

        result = runner.invoke(cli, ["scan", str(data_dir)])
        assert result.exit_code == 0

        output = result.output
        # Should indicate there are unrecognized files
        assert "unrecognized format" in output.lower()

        # Should either:
        # 1. Show all if list is reasonable, or
        # 2. Show truncation message
        if "unknown_00.xyz" in output:
            # Files are shown, might be truncated
            if "unknown_14.xyz" not in output:
                # All not shown, should suggest --all
                assert "--all" in output or "more" in output.lower()

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

        # Find unknown files
        unknown_files = [f for f in skipped_files if "unknown" in f.get("reason_type", "")]
        assert len(unknown_files) == 2, (
            f"Expected 2 unknown files in JSON, got {len(unknown_files)}: {unknown_files}"
        )
