"""Tests for metadata.yaml data defaults (temporal, raster.nodata).

These tests verify that metadata.yaml can specify default values for
data properties that couldn't be auto-extracted from source files.

Per ADR-0038: metadata.yaml is the human enrichment layer. This extends
it to support "data defaults" for when auto-extraction fails.
"""

import pytest

from portolan_cli.metadata_yaml import (
    NodataMismatchError,
    apply_raster_nodata_defaults,
    apply_temporal_defaults,
    generate_metadata_template,
    validate_metadata,
)


class TestMetadataDefaultsValidation:
    """Test validation of the 'defaults' section in metadata.yaml."""

    def test_defaults_section_is_optional(self) -> None:
        """Metadata without defaults section is valid."""
        metadata = {
            "contact": {"name": "Test", "email": "test@example.com"},
            "license": "CC-BY-4.0",
        }
        errors = validate_metadata(metadata)
        assert errors == []

    def test_valid_temporal_year_default(self) -> None:
        """Temporal defaults with year are valid."""
        metadata = {
            "contact": {"name": "Test", "email": "test@example.com"},
            "license": "CC-BY-4.0",
            "defaults": {
                "temporal": {
                    "year": 2025,
                }
            },
        }
        errors = validate_metadata(metadata)
        assert errors == []

    def test_valid_temporal_range_default(self) -> None:
        """Temporal defaults with start/end are valid."""
        metadata = {
            "contact": {"name": "Test", "email": "test@example.com"},
            "license": "CC-BY-4.0",
            "defaults": {
                "temporal": {
                    "start": "2025-04-15",
                    "end": "2025-05-30",
                }
            },
        }
        errors = validate_metadata(metadata)
        assert errors == []

    def test_valid_raster_nodata_default(self) -> None:
        """Raster nodata default is valid."""
        metadata = {
            "contact": {"name": "Test", "email": "test@example.com"},
            "license": "CC-BY-4.0",
            "defaults": {
                "raster": {
                    "nodata": 0,
                }
            },
        }
        errors = validate_metadata(metadata)
        assert errors == []

    def test_valid_raster_nodata_per_band(self) -> None:
        """Per-band nodata values are valid."""
        metadata = {
            "contact": {"name": "Test", "email": "test@example.com"},
            "license": "CC-BY-4.0",
            "defaults": {
                "raster": {
                    "nodata": [0, 0, 255],  # Per-band: R=0, G=0, B=255
                }
            },
        }
        errors = validate_metadata(metadata)
        assert errors == []

    def test_invalid_temporal_year_type(self) -> None:
        """Temporal year must be an integer."""
        metadata = {
            "contact": {"name": "Test", "email": "test@example.com"},
            "license": "CC-BY-4.0",
            "defaults": {
                "temporal": {
                    "year": "2025",  # String instead of int
                }
            },
        }
        errors = validate_metadata(metadata)
        assert any("year" in e.lower() and "integer" in e.lower() for e in errors)

    def test_invalid_temporal_date_format(self) -> None:
        """Temporal start/end must be valid ISO dates."""
        metadata = {
            "contact": {"name": "Test", "email": "test@example.com"},
            "license": "CC-BY-4.0",
            "defaults": {
                "temporal": {
                    "start": "04-15-2025",  # Wrong format (MM-DD-YYYY)
                    "end": "2025-05-30",
                }
            },
        }
        errors = validate_metadata(metadata)
        assert any("date" in e.lower() or "format" in e.lower() for e in errors)

    def test_invalid_raster_nodata_type(self) -> None:
        """Raster nodata must be number or list of numbers."""
        metadata = {
            "contact": {"name": "Test", "email": "test@example.com"},
            "license": "CC-BY-4.0",
            "defaults": {
                "raster": {
                    "nodata": "zero",  # String instead of number
                }
            },
        }
        errors = validate_metadata(metadata)
        assert any("nodata" in e.lower() for e in errors)

    def test_invalid_year_out_of_range_low(self) -> None:
        """Year must be >= 1800."""
        metadata = {
            "contact": {"name": "Test", "email": "test@example.com"},
            "license": "CC-BY-4.0",
            "defaults": {"temporal": {"year": 1700}},
        }
        errors = validate_metadata(metadata)
        assert any("1800" in e and "2100" in e for e in errors)

    def test_invalid_year_out_of_range_high(self) -> None:
        """Year must be <= 2100."""
        metadata = {
            "contact": {"name": "Test", "email": "test@example.com"},
            "license": "CC-BY-4.0",
            "defaults": {"temporal": {"year": 2200}},
        }
        errors = validate_metadata(metadata)
        assert any("1800" in e and "2100" in e for e in errors)

    def test_invalid_date_impossible_month(self) -> None:
        """Date with month 13 should fail validation."""
        metadata = {
            "contact": {"name": "Test", "email": "test@example.com"},
            "license": "CC-BY-4.0",
            "defaults": {"temporal": {"start": "2025-13-01"}},
        }
        errors = validate_metadata(metadata)
        # Should fail regex (month is 2 digits but must be 01-12)
        assert any("date" in e.lower() or "format" in e.lower() for e in errors)

    def test_invalid_date_impossible_day(self) -> None:
        """Date with day 32 should fail validation."""
        metadata = {
            "contact": {"name": "Test", "email": "test@example.com"},
            "license": "CC-BY-4.0",
            "defaults": {"temporal": {"start": "2025-01-32"}},
        }
        errors = validate_metadata(metadata)
        assert any("does not exist" in e.lower() or "date" in e.lower() for e in errors)

    def test_invalid_date_feb_30(self) -> None:
        """February 30 should fail validation."""
        metadata = {
            "contact": {"name": "Test", "email": "test@example.com"},
            "license": "CC-BY-4.0",
            "defaults": {"temporal": {"start": "2025-02-30"}},
        }
        errors = validate_metadata(metadata)
        assert any("does not exist" in e.lower() for e in errors)

    def test_both_year_and_start_is_error(self) -> None:
        """Specifying both year and start should produce an error."""
        metadata = {
            "contact": {"name": "Test", "email": "test@example.com"},
            "license": "CC-BY-4.0",
            "defaults": {"temporal": {"year": 2025, "start": "2025-06-01"}},
        }
        errors = validate_metadata(metadata)
        assert any("both" in e.lower() and "year" in e.lower() for e in errors)

    def test_invalid_nodata_nan(self) -> None:
        """NaN nodata should fail validation."""
        metadata = {
            "contact": {"name": "Test", "email": "test@example.com"},
            "license": "CC-BY-4.0",
            "defaults": {"raster": {"nodata": float("nan")}},
        }
        errors = validate_metadata(metadata)
        assert any("nan" in e.lower() or "finite" in e.lower() for e in errors)

    def test_invalid_nodata_infinity(self) -> None:
        """Infinity nodata should fail validation."""
        metadata = {
            "contact": {"name": "Test", "email": "test@example.com"},
            "license": "CC-BY-4.0",
            "defaults": {"raster": {"nodata": float("inf")}},
        }
        errors = validate_metadata(metadata)
        assert any("infinity" in e.lower() or "finite" in e.lower() for e in errors)

    def test_invalid_nodata_list_with_nan(self) -> None:
        """Per-band nodata with NaN should fail validation."""
        metadata = {
            "contact": {"name": "Test", "email": "test@example.com"},
            "license": "CC-BY-4.0",
            "defaults": {"raster": {"nodata": [0, float("nan"), 255]}},
        }
        errors = validate_metadata(metadata)
        assert any("nan" in e.lower() or "finite" in e.lower() for e in errors)

    def test_invalid_nodata_empty_list(self) -> None:
        """Empty nodata list should fail validation."""
        metadata = {
            "contact": {"name": "Test", "email": "test@example.com"},
            "license": "CC-BY-4.0",
            "defaults": {"raster": {"nodata": []}},
        }
        errors = validate_metadata(metadata)
        assert any("empty" in e.lower() for e in errors)


class TestMetadataTemplateIncludesDefaults:
    """Test that the metadata template includes the defaults section."""

    def test_template_includes_defaults_section(self) -> None:
        """Generated template includes commented defaults section."""
        template = generate_metadata_template()
        assert "defaults:" in template or "OPTIONAL: Data defaults" in template

    def test_template_includes_temporal_example(self) -> None:
        """Template shows temporal defaults example."""
        template = generate_metadata_template()
        assert "temporal" in template.lower()
        assert "year" in template.lower() or "start" in template.lower()

    def test_template_includes_raster_nodata_example(self) -> None:
        """Template shows raster nodata example."""
        template = generate_metadata_template()
        assert "nodata" in template.lower()


class TestApplyTemporalDefaults:
    """Test applying temporal defaults to items."""

    def test_year_default_returns_datetime_range(self) -> None:
        """Year default produces start of year datetime."""
        defaults = {"temporal": {"year": 2025}}
        result = apply_temporal_defaults(defaults)

        assert result is not None
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 1

    def test_start_date_default(self) -> None:
        """Start date produces datetime."""
        defaults = {"temporal": {"start": "2025-04-15"}}
        result = apply_temporal_defaults(defaults)

        assert result is not None
        assert result.year == 2025
        assert result.month == 4
        assert result.day == 15

    def test_no_temporal_defaults_returns_none(self) -> None:
        """Missing temporal defaults returns None."""
        defaults = {"raster": {"nodata": 0}}
        result = apply_temporal_defaults(defaults)
        assert result is None

    def test_empty_defaults_returns_none(self) -> None:
        """Empty defaults dict returns None."""
        result = apply_temporal_defaults({})
        assert result is None

    def test_year_takes_precedence_over_start(self) -> None:
        """If both year and start specified, year takes precedence."""
        defaults = {"temporal": {"year": 2025, "start": "2024-06-15"}}
        result = apply_temporal_defaults(defaults)

        # Year should take precedence
        assert result is not None
        assert result.year == 2025
        assert result.month == 1

    def test_invalid_date_raises_value_error(self) -> None:
        """Invalid date string raises ValueError."""
        defaults = {"temporal": {"start": "2025-02-30"}}  # Feb 30 doesn't exist

        with pytest.raises(ValueError) as exc_info:
            apply_temporal_defaults(defaults)

        assert "invalid" in str(exc_info.value).lower() or "2025-02-30" in str(exc_info.value)

    def test_invalid_year_type_raises_value_error(self) -> None:
        """Non-integer year raises ValueError."""
        defaults = {"temporal": {"year": "2025"}}  # String instead of int

        with pytest.raises(ValueError) as exc_info:
            apply_temporal_defaults(defaults)

        assert "integer" in str(exc_info.value).lower()


class TestApplyRasterNodataDefaults:
    """Test applying raster nodata defaults."""

    def test_uniform_nodata_default(self) -> None:
        """Uniform nodata applies to all bands."""
        defaults = {"raster": {"nodata": 0}}
        nodatavals = (None, None, None)  # 3 bands, no nodata

        result = apply_raster_nodata_defaults(defaults, nodatavals, band_count=3)

        assert result == (0.0, 0.0, 0.0)

    def test_per_band_nodata_default(self) -> None:
        """Per-band nodata applies to each band."""
        defaults = {"raster": {"nodata": [0, 0, 255]}}
        nodatavals = (None, None, None)

        result = apply_raster_nodata_defaults(defaults, nodatavals, band_count=3)

        assert result == (0.0, 0.0, 255.0)

    def test_existing_nodata_not_overridden(self) -> None:
        """Existing nodata values are preserved, not overridden."""
        defaults = {"raster": {"nodata": 0}}
        nodatavals = (255, None, None)  # Band 1 already has nodata

        result = apply_raster_nodata_defaults(defaults, nodatavals, band_count=3)

        # Band 1 keeps 255.0, bands 2-3 get default 0.0
        assert result == (255.0, 0.0, 0.0)

    def test_no_raster_defaults_returns_original(self) -> None:
        """Missing raster defaults returns original nodatavals."""
        defaults = {"temporal": {"year": 2025}}
        nodatavals = (None, None, None)

        result = apply_raster_nodata_defaults(defaults, nodatavals, band_count=3)

        assert result == (None, None, None)

    def test_none_nodatavals_with_defaults(self) -> None:
        """None nodatavals tuple gets populated from defaults."""
        defaults = {"raster": {"nodata": 0}}

        result = apply_raster_nodata_defaults(defaults, None, band_count=3)

        assert result == (0.0, 0.0, 0.0)

    def test_per_band_nodata_length_mismatch_raises_error(self) -> None:
        """Per-band nodata with wrong length raises error in strict mode."""
        defaults = {"raster": {"nodata": [0, 128]}}  # Only 2 values for 3 bands
        nodatavals = (None, None, None)

        with pytest.raises(NodataMismatchError) as exc_info:
            apply_raster_nodata_defaults(defaults, nodatavals, band_count=3, strict=True)

        assert "2 values" in str(exc_info.value)
        assert "3 bands" in str(exc_info.value)

    def test_per_band_nodata_length_mismatch_pads_non_strict(self) -> None:
        """Per-band nodata shorter than band count pads with last value in non-strict mode."""
        defaults = {"raster": {"nodata": [0, 128]}}  # Only 2 values for 3 bands
        nodatavals = (None, None, None)

        # Non-strict mode pads with last value
        result = apply_raster_nodata_defaults(defaults, nodatavals, band_count=3, strict=False)

        # Should use 128 (last value) for band 3
        assert result == (0.0, 128.0, 128.0)

    def test_per_band_nodata_too_long_raises_error(self) -> None:
        """Per-band nodata with too many values raises error."""
        defaults = {"raster": {"nodata": [0, 128, 255, 127]}}  # 4 values for 3 bands
        nodatavals = (None, None, None)

        with pytest.raises(NodataMismatchError) as exc_info:
            apply_raster_nodata_defaults(defaults, nodatavals, band_count=3, strict=True)

        assert "4 values" in str(exc_info.value)
        assert "3 bands" in str(exc_info.value)

    def test_nodata_returns_floats_for_consistency(self) -> None:
        """Nodata values are returned as floats for type consistency."""
        defaults = {"raster": {"nodata": 0}}  # Integer input
        nodatavals = (None, None, None)

        result = apply_raster_nodata_defaults(defaults, nodatavals, band_count=3)

        # Should be floats, not ints
        assert result == (0.0, 0.0, 0.0)
        assert all(isinstance(v, float) for v in result)
