"""Tests for statistics configuration.

Per ADR-0034: Stats enabled by default, configurable via .portolan/config.yaml
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.config import KNOWN_SETTINGS, get_setting


class TestStatisticsConfigSettings:
    """Tests for statistics config settings."""

    @pytest.mark.unit
    def test_statistics_enabled_is_known_setting(self) -> None:
        """statistics.enabled should be a known config setting."""
        assert "statistics.enabled" in KNOWN_SETTINGS

    @pytest.mark.unit
    def test_statistics_raster_mode_is_known_setting(self) -> None:
        """statistics.raster_mode should be a known config setting."""
        assert "statistics.raster_mode" in KNOWN_SETTINGS

    @pytest.mark.unit
    def test_statistics_enabled_default_is_true(self, tmp_path: Path) -> None:
        """statistics.enabled should default to True."""
        # Without any config file, should default to True
        result = get_setting("statistics.enabled", catalog_path=tmp_path)
        assert result is True

    @pytest.mark.unit
    def test_statistics_raster_mode_default_is_approx(self, tmp_path: Path) -> None:
        """statistics.raster_mode should default to 'approx'."""
        result = get_setting("statistics.raster_mode", catalog_path=tmp_path)
        assert result == "approx"
