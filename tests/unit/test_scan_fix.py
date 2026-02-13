"""Unit tests for portolan_cli/scan_fix.py.

Tests fix mode implementations: safe fixes, unsafe fixes, dry-run.
"""

from __future__ import annotations

import pytest

from portolan_cli.scan_fix import (
    FixCategory,
)


@pytest.mark.unit
class TestFixCategory:
    """Tests for FixCategory enum."""

    def test_has_three_categories(self) -> None:
        """FixCategory should have exactly 3 categories."""
        assert len(FixCategory) == 3

    def test_category_values(self) -> None:
        """FixCategory should have expected values."""
        assert FixCategory.SAFE.value == "safe"
        assert FixCategory.UNSAFE.value == "unsafe"
        assert FixCategory.MANUAL.value == "manual"


@pytest.mark.unit
class TestSafeFixes:
    """Tests for safe fix operations.

    TODO: Add tests for _compute_safe_rename
    TODO: Add tests for apply_safe_fixes
    """

    def test_placeholder(self) -> None:
        """Placeholder test - safe fix tests to be implemented."""
        # This is a placeholder to ensure the test class is not empty.
        # Real tests should be added when safe fix operations are implemented.
        pytest.skip("Safe fix tests not yet implemented")


@pytest.mark.unit
class TestUnsafeFixes:
    """Tests for unsafe fix operations.

    TODO: Add tests for _compute_unsafe_split
    TODO: Add tests for _compute_unsafe_rename
    TODO: Add tests for apply_unsafe_fixes
    """

    def test_placeholder(self) -> None:
        """Placeholder test - unsafe fix tests to be implemented."""
        # This is a placeholder to ensure the test class is not empty.
        # Real tests should be added when unsafe fix operations are implemented.
        pytest.skip("Unsafe fix tests not yet implemented")
