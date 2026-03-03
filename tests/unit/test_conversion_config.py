"""Unit tests for conversion configuration.

Tests the conversion config system that allows users to override default
format handling behavior:
- Force-convert cloud-native formats (e.g., FlatGeobuf -> GeoParquet)
- Preserve convertible formats (e.g., keep Shapefiles as-is)
- Path-based overrides with glob patterns

See GitHub Issue #75 for context on FlatGeobuf handling.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.config import (
    get_setting,
    load_config,
    set_setting,
)


class TestConversionConfigParsing:
    """Tests for parsing conversion config from .portolan/config.yaml."""

    @pytest.mark.unit
    def test_load_conversion_extensions_convert(self, tmp_path: Path) -> None:
        """Conversion config with extensions.convert list is loaded correctly."""
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        config_file = portolan_dir / "config.yaml"
        config_file.write_text("""
conversion:
  extensions:
    convert: [fgb, gpkg]
""")
        config = load_config(tmp_path)
        assert config["conversion"]["extensions"]["convert"] == ["fgb", "gpkg"]

    @pytest.mark.unit
    def test_load_conversion_extensions_preserve(self, tmp_path: Path) -> None:
        """Conversion config with extensions.preserve list is loaded correctly."""
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        config_file = portolan_dir / "config.yaml"
        config_file.write_text("""
conversion:
  extensions:
    preserve: [shp, gpkg]
""")
        config = load_config(tmp_path)
        assert config["conversion"]["extensions"]["preserve"] == ["shp", "gpkg"]

    @pytest.mark.unit
    def test_load_conversion_paths_preserve(self, tmp_path: Path) -> None:
        """Conversion config with paths.preserve list is loaded correctly."""
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        config_file = portolan_dir / "config.yaml"
        config_file.write_text("""
conversion:
  paths:
    preserve:
      - "legacy/**"
      - "regulatory/*.shp"
""")
        config = load_config(tmp_path)
        assert config["conversion"]["paths"]["preserve"] == [
            "legacy/**",
            "regulatory/*.shp",
        ]

    @pytest.mark.unit
    def test_load_full_conversion_config(self, tmp_path: Path) -> None:
        """Full conversion config with all options is loaded correctly."""
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        config_file = portolan_dir / "config.yaml"
        config_file.write_text("""
remote: s3://bucket/
conversion:
  extensions:
    convert: [fgb]
    preserve: [gpkg]
  paths:
    preserve:
      - "archive/**"
""")
        config = load_config(tmp_path)
        assert config["remote"] == "s3://bucket/"
        assert config["conversion"]["extensions"]["convert"] == ["fgb"]
        assert config["conversion"]["extensions"]["preserve"] == ["gpkg"]
        assert config["conversion"]["paths"]["preserve"] == ["archive/**"]

    @pytest.mark.unit
    def test_missing_conversion_config_returns_empty(self, tmp_path: Path) -> None:
        """Missing conversion section returns empty dict (not error)."""
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        config_file = portolan_dir / "config.yaml"
        config_file.write_text("remote: s3://bucket/\n")
        config = load_config(tmp_path)
        assert "conversion" not in config

    @pytest.mark.unit
    def test_get_conversion_setting(self, tmp_path: Path) -> None:
        """Conversion config can be retrieved via get_setting()."""
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        config_file = portolan_dir / "config.yaml"
        config_file.write_text("""
conversion:
  extensions:
    convert: [fgb]
""")
        result = get_setting("conversion", catalog_path=tmp_path)
        assert result == {"extensions": {"convert": ["fgb"]}}

    @pytest.mark.unit
    def test_set_conversion_setting(self, tmp_path: Path) -> None:
        """Conversion config can be set via set_setting()."""
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()

        set_setting(
            tmp_path,
            "conversion",
            {"extensions": {"convert": ["fgb"], "preserve": ["gpkg"]}},
        )

        config = load_config(tmp_path)
        assert config["conversion"]["extensions"]["convert"] == ["fgb"]
        assert config["conversion"]["extensions"]["preserve"] == ["gpkg"]


class TestConversionConfigHelpers:
    """Tests for conversion config helper functions."""

    @pytest.mark.unit
    def test_get_conversion_overrides_returns_parsed_config(self, tmp_path: Path) -> None:
        """get_conversion_overrides() returns parsed config structure."""
        from portolan_cli.conversion_config import get_conversion_overrides

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        config_file = portolan_dir / "config.yaml"
        config_file.write_text("""
conversion:
  extensions:
    convert: [fgb]
    preserve: [gpkg, shp]
  paths:
    preserve:
      - "legacy/**"
""")
        overrides = get_conversion_overrides(tmp_path)

        assert overrides.extensions_convert == {".fgb"}
        assert overrides.extensions_preserve == {".gpkg", ".shp"}
        assert overrides.paths_preserve == ("legacy/**",)

    @pytest.mark.unit
    def test_get_conversion_overrides_empty_catalog(self, tmp_path: Path) -> None:
        """get_conversion_overrides() returns empty overrides for missing config."""
        from portolan_cli.conversion_config import get_conversion_overrides

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()

        overrides = get_conversion_overrides(tmp_path)

        assert overrides.extensions_convert == set()
        assert overrides.extensions_preserve == set()
        assert overrides.paths_preserve == ()

    @pytest.mark.unit
    def test_get_conversion_overrides_normalizes_extensions(self, tmp_path: Path) -> None:
        """get_conversion_overrides() normalizes extensions to lowercase with dot."""
        from portolan_cli.conversion_config import get_conversion_overrides

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        config_file = portolan_dir / "config.yaml"
        # Extensions without dots, mixed case
        config_file.write_text("""
conversion:
  extensions:
    convert: [FGB, .Gpkg]
""")
        overrides = get_conversion_overrides(tmp_path)

        assert overrides.extensions_convert == {".fgb", ".gpkg"}


class TestConversionOverridesDataclass:
    """Tests for the ConversionOverrides dataclass."""

    @pytest.mark.unit
    def test_should_force_convert_extension(self) -> None:
        """should_force_convert() returns True for extensions in convert list."""
        from portolan_cli.conversion_config import ConversionOverrides

        overrides = ConversionOverrides(
            extensions_convert={".fgb"},
            extensions_preserve=set(),
            paths_preserve=[],
        )

        assert overrides.should_force_convert(Path("test.fgb")) is True
        assert overrides.should_force_convert(Path("test.gpkg")) is False

    @pytest.mark.unit
    def test_should_preserve_extension(self) -> None:
        """should_preserve() returns True for extensions in preserve list."""
        from portolan_cli.conversion_config import ConversionOverrides

        overrides = ConversionOverrides(
            extensions_convert=set(),
            extensions_preserve={".gpkg", ".shp"},
            paths_preserve=[],
        )

        assert overrides.should_preserve(Path("test.gpkg")) is True
        assert overrides.should_preserve(Path("test.shp")) is True
        assert overrides.should_preserve(Path("test.geojson")) is False

    @pytest.mark.unit
    def test_should_preserve_path_pattern(self, tmp_path: Path) -> None:
        """should_preserve() returns True for files matching path patterns."""
        from portolan_cli.conversion_config import ConversionOverrides

        overrides = ConversionOverrides(
            extensions_convert=set(),
            extensions_preserve=set(),
            paths_preserve=["legacy/**", "regulatory/*.shp"],
        )

        # Files in legacy/ should be preserved
        legacy_file = tmp_path / "legacy" / "old_data.shp"
        assert overrides.should_preserve(legacy_file, root=tmp_path) is True

        # Files in regulatory/ with .shp should be preserved
        regulatory_file = tmp_path / "regulatory" / "boundaries.shp"
        assert overrides.should_preserve(regulatory_file, root=tmp_path) is True

        # Files in regulatory/ with other extensions should NOT be preserved
        regulatory_other = tmp_path / "regulatory" / "boundaries.geojson"
        assert overrides.should_preserve(regulatory_other, root=tmp_path) is False

        # Files outside preserved paths should NOT be preserved
        other_file = tmp_path / "data" / "boundaries.shp"
        assert overrides.should_preserve(other_file, root=tmp_path) is False

    @pytest.mark.unit
    def test_path_pattern_takes_precedence_over_extension(self, tmp_path: Path) -> None:
        """Path patterns should override extension-based rules."""
        from portolan_cli.conversion_config import ConversionOverrides

        overrides = ConversionOverrides(
            extensions_convert={".fgb"},  # Normally convert FlatGeobuf
            extensions_preserve=set(),
            paths_preserve=["archive/**"],  # But preserve everything in archive/
        )

        # FlatGeobuf in archive/ should be preserved (path wins)
        archive_fgb = tmp_path / "archive" / "data.fgb"
        assert overrides.should_preserve(archive_fgb, root=tmp_path) is True

        # FlatGeobuf outside archive/ should be converted
        other_fgb = tmp_path / "data" / "data.fgb"
        assert overrides.should_force_convert(other_fgb) is True
        assert overrides.should_preserve(other_fgb, root=tmp_path) is False

    @pytest.mark.unit
    def test_should_preserve_path_not_relative_to_root(self, tmp_path: Path) -> None:
        """should_preserve() handles paths not relative to root gracefully."""
        from portolan_cli.conversion_config import ConversionOverrides

        overrides = ConversionOverrides(
            extensions_convert=frozenset(),
            extensions_preserve=frozenset({".shp"}),
            paths_preserve=("archive/**",),
        )

        # Create a path completely outside root
        unrelated_path = Path("/some/other/location/data.fgb")

        # Should not raise, should fall through to extension check
        result = overrides.should_preserve(unrelated_path, root=tmp_path)
        assert result is False  # .fgb not in extensions_preserve


class TestConversionConfigEdgeCases:
    """Tests for edge cases in config parsing."""

    @pytest.mark.unit
    def test_conversion_not_a_dict(self, tmp_path: Path) -> None:
        """get_conversion_overrides() handles non-dict conversion value."""
        from portolan_cli.conversion_config import get_conversion_overrides

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        config_file = portolan_dir / "config.yaml"
        # conversion is a string, not a dict
        config_file.write_text("conversion: invalid_value\n")

        overrides = get_conversion_overrides(tmp_path)

        # Should return empty overrides
        assert overrides.extensions_convert == frozenset()
        assert overrides.extensions_preserve == frozenset()
        assert overrides.paths_preserve == ()

    @pytest.mark.unit
    def test_extensions_not_a_dict(self, tmp_path: Path) -> None:
        """get_conversion_overrides() handles non-dict extensions value."""
        from portolan_cli.conversion_config import get_conversion_overrides

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        config_file = portolan_dir / "config.yaml"
        # extensions is a list, not a dict
        config_file.write_text("""
conversion:
  extensions: [fgb, shp]
""")
        overrides = get_conversion_overrides(tmp_path)

        # Should return empty extensions
        assert overrides.extensions_convert == frozenset()
        assert overrides.extensions_preserve == frozenset()

    @pytest.mark.unit
    def test_paths_not_a_dict(self, tmp_path: Path) -> None:
        """get_conversion_overrides() handles non-dict paths value."""
        from portolan_cli.conversion_config import get_conversion_overrides

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        config_file = portolan_dir / "config.yaml"
        # paths is a string, not a dict
        config_file.write_text("""
conversion:
  paths: archive/**
""")
        overrides = get_conversion_overrides(tmp_path)

        # Should return empty paths
        assert overrides.paths_preserve == ()

    @pytest.mark.unit
    def test_non_string_items_in_convert_list(self, tmp_path: Path) -> None:
        """get_conversion_overrides() filters non-string items from convert list."""
        from portolan_cli.conversion_config import get_conversion_overrides

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        config_file = portolan_dir / "config.yaml"
        # Mix of valid strings and invalid types
        config_file.write_text("""
conversion:
  extensions:
    convert:
      - fgb
      - 123
      - null
      - gpkg
""")
        overrides = get_conversion_overrides(tmp_path)

        # Should only include valid string extensions
        assert overrides.extensions_convert == frozenset({".fgb", ".gpkg"})

    @pytest.mark.unit
    def test_non_string_items_in_preserve_list(self, tmp_path: Path) -> None:
        """get_conversion_overrides() filters non-string items from preserve list."""
        from portolan_cli.conversion_config import get_conversion_overrides

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        config_file = portolan_dir / "config.yaml"
        # Mix of valid strings and invalid types
        config_file.write_text("""
conversion:
  extensions:
    preserve:
      - shp
      - 456
      - true
      - geojson
""")
        overrides = get_conversion_overrides(tmp_path)

        # Should only include valid string extensions
        assert overrides.extensions_preserve == frozenset({".shp", ".geojson"})
