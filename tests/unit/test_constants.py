"""Tests for constants module."""

from portolan_cli.constants import MAX_CATALOG_DEPTH


def test_max_catalog_depth_constant_exists():
    """Test that MAX_CATALOG_DEPTH constant is defined."""
    assert MAX_CATALOG_DEPTH == 10


def test_max_catalog_depth_is_reasonable():
    """Test that MAX_CATALOG_DEPTH is within a reasonable range."""
    assert MAX_CATALOG_DEPTH >= 5, "Should allow at least 5 levels of nesting"
    assert MAX_CATALOG_DEPTH <= 20, "Should prevent excessive nesting"
