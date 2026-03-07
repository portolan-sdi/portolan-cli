"""Integration tests for ISO country code handling in scan command.

Per ADR-0030, scan should not warn on uppercase directory names that are
valid ISO 3166-1 alpha-3 country codes or disputed territory patterns.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


@pytest.fixture
def iso_code_structure(tmp_path: Path) -> Path:
    """Create a directory structure with ISO country code directories."""
    # Create ISO code directories with geo files
    for code in ["USA", "GBR", "CHN"]:
        code_dir = tmp_path / code
        code_dir.mkdir()
        # Create a minimal GeoJSON file
        geojson = code_dir / f"{code}.geojson"
        geojson.write_text('{"type": "FeatureCollection", "features": []}')
    return tmp_path


@pytest.fixture
def disputed_territory_structure(tmp_path: Path) -> Path:
    """Create a directory structure with disputed territory code directories."""
    # Create disputed territory directories with geo files
    for code in ["xAB", "xJK", "xPI"]:
        code_dir = tmp_path / code
        code_dir.mkdir()
        geojson = code_dir / f"{code}.geojson"
        geojson.write_text('{"type": "FeatureCollection", "features": []}')
    return tmp_path


@pytest.fixture
def invalid_uppercase_structure(tmp_path: Path) -> Path:
    """Create a directory structure with invalid uppercase directories."""
    # Create non-ISO uppercase directories
    for code in ["FOO", "BAR", "XYZ"]:
        code_dir = tmp_path / code
        code_dir.mkdir()
        geojson = code_dir / f"{code}.geojson"
        geojson.write_text('{"type": "FeatureCollection", "features": []}')
    return tmp_path


@pytest.fixture
def mixed_code_structure(tmp_path: Path) -> Path:
    """Create a structure with both valid ISO codes and invalid uppercase."""
    # Valid ISO codes
    for code in ["USA", "GBR"]:
        code_dir = tmp_path / code
        code_dir.mkdir()
        geojson = code_dir / f"{code}.geojson"
        geojson.write_text('{"type": "FeatureCollection", "features": []}')
    # Invalid uppercase
    for code in ["FOO"]:
        code_dir = tmp_path / code
        code_dir.mkdir()
        geojson = code_dir / f"{code}.geojson"
        geojson.write_text('{"type": "FeatureCollection", "features": []}')
    return tmp_path


@pytest.mark.integration
class TestScanIsoCodeHandling:
    """Integration tests for ISO code handling in scan command."""

    def test_iso_codes_no_warnings(self, iso_code_structure: Path) -> None:
        """Scan should not warn on valid ISO country code directories."""
        runner = CliRunner()
        result = runner.invoke(cli, ["scan", str(iso_code_structure), "--json"])

        assert result.exit_code == 0
        data = json.loads(result.output)

        # No warnings about invalid collection IDs
        issues = data["data"].get("issues", [])
        id_warnings = [i for i in issues if i.get("type") == "invalid_collection_id"]
        assert len(id_warnings) == 0, f"Unexpected ID warnings: {id_warnings}"

    def test_disputed_territories_no_warnings(self, disputed_territory_structure: Path) -> None:
        """Scan should not warn on disputed territory pattern directories."""
        runner = CliRunner()
        result = runner.invoke(cli, ["scan", str(disputed_territory_structure), "--json"])

        assert result.exit_code == 0
        data = json.loads(result.output)

        # No warnings about invalid collection IDs
        issues = data["data"].get("issues", [])
        id_warnings = [i for i in issues if i.get("type") == "invalid_collection_id"]
        assert len(id_warnings) == 0, f"Unexpected ID warnings: {id_warnings}"

    def test_invalid_uppercase_still_warns(self, invalid_uppercase_structure: Path) -> None:
        """Scan should still warn on non-ISO uppercase directories."""
        runner = CliRunner()
        result = runner.invoke(cli, ["scan", str(invalid_uppercase_structure), "--json"])

        assert result.exit_code == 0
        data = json.loads(result.output)

        # Should have warnings about invalid collection IDs
        issues = data["data"].get("issues", [])
        id_warnings = [i for i in issues if i.get("type") == "invalid_collection_id"]
        # Should warn for FOO, BAR, XYZ (3 warnings)
        assert len(id_warnings) == 3, f"Expected 3 warnings, got: {id_warnings}"

    def test_mixed_codes_only_warns_invalid(self, mixed_code_structure: Path) -> None:
        """Scan should only warn on invalid codes, not valid ISO codes."""
        runner = CliRunner()
        result = runner.invoke(cli, ["scan", str(mixed_code_structure), "--json"])

        assert result.exit_code == 0
        data = json.loads(result.output)

        # Should have exactly 1 warning (for FOO only)
        issues = data["data"].get("issues", [])
        id_warnings = [i for i in issues if i.get("type") == "invalid_collection_id"]
        assert len(id_warnings) == 1
        assert "FOO" in id_warnings[0].get("relative_path", "")

    def test_human_readable_no_iso_warnings(self, iso_code_structure: Path) -> None:
        """Human-readable output should not mention ISO codes as invalid."""
        runner = CliRunner()
        result = runner.invoke(cli, ["scan", str(iso_code_structure)])

        assert result.exit_code == 0
        # Should not contain uppercase warning for ISO codes
        assert "Invalid collection ID" not in result.output
        assert "uppercase" not in result.output.lower() or "must be lowercase" not in result.output
