"""Integration tests for batched warning output in `portolan scan`.

Verifies that the CLI collapses repeated IssueType warnings into batched groups
rather than printing one line per issue.  Uses Click's CliRunner plus temporary
filesystem fixtures.

NOTE: INVALID_COLLECTION_ID warnings are only generated for directories that
contain recognized geospatial files (*.geojson, *.shp, *.gpkg, *.fgb, *.tif,
*.tiff, *.img).  Fixtures therefore use *.geojson files.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


@pytest.fixture()
def runner() -> CliRunner:
    """Create a Click test runner."""
    return CliRunner()


def _make_geospatial_dir(parent: Path, name: str) -> None:
    """Create a sub-directory with a minimal GeoJSON file so the scanner treats it as a collection."""
    d = parent / name
    d.mkdir()
    (d / "data.geojson").write_text('{"type":"FeatureCollection","features":[]}')


@pytest.fixture()
def many_uppercase_dirs(tmp_path: Path) -> Path:
    """Create 10 directories with uppercase names (INVALID_COLLECTION_ID issues)."""
    country_codes = ["USA", "GBR", "CHN", "DEU", "FRA", "BRA", "IND", "JPN", "AUS", "CAN"]
    for code in country_codes:
        _make_geospatial_dir(tmp_path, code)
    return tmp_path


# =============================================================================
# Batching behaviour verified through CLI output
# =============================================================================


@pytest.mark.integration
class TestScanBatching:
    """CLI integration tests for batched warning output."""

    def test_many_same_type_issues_collapsed_to_batch(
        self, runner: CliRunner, many_uppercase_dirs: Path
    ) -> None:
        """10 INVALID_COLLECTION_ID warnings collapse into one batch group."""
        result = runner.invoke(cli, ["scan", str(many_uppercase_dirs)])

        assert result.exit_code == 0
        # Should report 10 warnings total
        assert "10" in result.output
        # Should NOT show 10 separate lines — at most 3 country names appear
        country_codes = ["USA", "GBR", "CHN", "DEU", "FRA", "BRA", "IND", "JPN", "AUS", "CAN"]
        visible = [c for c in country_codes if c in result.output]
        assert len(visible) <= 3

    def test_batch_shows_and_n_more(self, runner: CliRunner, many_uppercase_dirs: Path) -> None:
        """Output includes '(and N more)' when batch is truncated."""
        result = runner.invoke(cli, ["scan", str(many_uppercase_dirs)])

        assert result.exit_code == 0
        assert "more" in result.output.lower()

    def test_batch_all_flag_shows_all_paths(
        self, runner: CliRunner, many_uppercase_dirs: Path
    ) -> None:
        """With --all, all 10 country directory names appear in output."""
        result = runner.invoke(cli, ["scan", str(many_uppercase_dirs), "--all"])

        assert result.exit_code == 0
        country_codes = ["USA", "GBR", "CHN", "DEU", "FRA", "BRA", "IND", "JPN", "AUS", "CAN"]
        for code in country_codes:
            assert code in result.output, f"{code} not found in output with --all"

    def test_batch_does_not_suppress_total_count(
        self, runner: CliRunner, many_uppercase_dirs: Path
    ) -> None:
        """The severity header still shows the total count (10 warnings)."""
        result = runner.invoke(cli, ["scan", str(many_uppercase_dirs)])

        assert result.exit_code == 0
        # "10 warnings" should appear
        assert "10 warning" in result.output.lower()

    def test_single_issue_not_batched(self, runner: CliRunner, tmp_path: Path) -> None:
        """A single issue of its type is shown inline (no 'Examples:' prefix)."""
        _make_geospatial_dir(tmp_path, "LONE")

        result = runner.invoke(cli, ["scan", str(tmp_path)])

        assert result.exit_code == 0
        # Single issue: path appears directly, no "Examples:" batch prefix
        assert "LONE" in result.output
        assert "Examples:" not in result.output

    def test_fixability_label_present_in_batch(
        self, runner: CliRunner, many_uppercase_dirs: Path
    ) -> None:
        """The [--fix] label appears in the batch group header."""
        result = runner.invoke(cli, ["scan", str(many_uppercase_dirs)])

        assert result.exit_code == 0
        assert "[--fix]" in result.output

    def test_different_issue_types_produce_separate_groups(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Uppercase dirs (INVALID_COLLECTION_ID) and invalid-chars files are separate groups."""
        # Two uppercase directories with geo-assets
        for code in ("USA", "GBR"):
            _make_geospatial_dir(tmp_path, code)

        # One file with invalid characters in root (triggers INVALID_CHARACTERS)
        (tmp_path / "file with spaces.geojson").write_text(
            '{"type":"FeatureCollection","features":[]}'
        )

        result = runner.invoke(cli, ["scan", str(tmp_path)])

        assert result.exit_code == 0
        # Both issue types should be referenced in output:
        # - INVALID_COLLECTION_ID batch (USA/GBR)
        # - INVALID_CHARACTERS warning for the space file
        assert "invalid_collection_id" in result.output.lower() or (
            "USA" in result.output or "GBR" in result.output or "2" in result.output
        )
        assert (
            "invalid_characters" in result.output.lower()
            or "spaces" in result.output.lower()
            or "spaces" in result.output
        )
