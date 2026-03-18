"""Tests for constants module."""

import pytest

from portolan_cli.constants import MAX_CATALOG_DEPTH

pytestmark = pytest.mark.unit


@pytest.mark.unit
def test_max_catalog_depth_constant_exists() -> None:
    """Test that MAX_CATALOG_DEPTH constant is defined."""
    assert MAX_CATALOG_DEPTH == 10


@pytest.mark.unit
def test_max_catalog_depth_is_reasonable() -> None:
    """Test that MAX_CATALOG_DEPTH is within a reasonable range."""
    assert MAX_CATALOG_DEPTH >= 5, "Should allow at least 5 levels of nesting"
    assert MAX_CATALOG_DEPTH <= 20, "Should prevent excessive nesting"
