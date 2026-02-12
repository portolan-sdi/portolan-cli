"""Unit tests for portolan_cli/scan_fix.py.

Tests fix mode implementations: safe fixes, unsafe fixes, dry-run.
"""

from __future__ import annotations

from portolan_cli.scan_fix import (
    FixCategory,
)


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


class TestSafeFixes:
    """Tests for safe fix operations."""

    # TODO: Add tests for _compute_safe_rename
    # TODO: Add tests for apply_safe_fixes
    pass


class TestUnsafeFixes:
    """Tests for unsafe fix operations."""

    # TODO: Add tests for _compute_unsafe_split
    # TODO: Add tests for _compute_unsafe_rename
    # TODO: Add tests for apply_unsafe_fixes
    pass
