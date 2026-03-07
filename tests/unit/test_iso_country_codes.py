"""Tests for ISO country code handling in scan validation.

Per ADR-0030, scan should not warn on uppercase directory names that are:
1. Valid ISO 3166-1 alpha-3 country codes (USA, GBR, CHN, etc.)
2. Disputed territory patterns (xAB, xJK, etc.)

Random uppercase names (FOO, BAR) should still trigger warnings.
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from portolan_cli.constants import ISO_ALPHA3_CODES, is_valid_uppercase_id


class TestIsValidUppercaseId:
    """Tests for the is_valid_uppercase_id function."""

    def test_standard_iso_code_usa(self) -> None:
        """USA is a valid ISO code."""
        assert is_valid_uppercase_id("USA") is True

    def test_standard_iso_code_gbr(self) -> None:
        """GBR is a valid ISO code."""
        assert is_valid_uppercase_id("GBR") is True

    def test_standard_iso_code_chn(self) -> None:
        """CHN is a valid ISO code."""
        assert is_valid_uppercase_id("CHN") is True

    def test_disputed_territory_xab(self) -> None:
        """xAB follows disputed territory pattern."""
        assert is_valid_uppercase_id("xAB") is True

    def test_disputed_territory_xjk(self) -> None:
        """xJK follows disputed territory pattern."""
        assert is_valid_uppercase_id("xJK") is True

    def test_random_uppercase_foo(self) -> None:
        """FOO is not a valid ISO code."""
        assert is_valid_uppercase_id("FOO") is False

    def test_random_uppercase_bar(self) -> None:
        """BAR is not a valid ISO code."""
        assert is_valid_uppercase_id("BAR") is False

    def test_lowercase_usa(self) -> None:
        """Lowercase 'usa' is not in ISO_ALPHA3_CODES (which are uppercase)."""
        assert is_valid_uppercase_id("usa") is False

    def test_mixed_case(self) -> None:
        """Mixed case 'Usa' is not valid."""
        assert is_valid_uppercase_id("Usa") is False

    def test_invalid_disputed_pattern_xab_lowercase(self) -> None:
        """xab (all lowercase) doesn't match disputed pattern."""
        assert is_valid_uppercase_id("xab") is False

    def test_invalid_disputed_pattern_XAB(self) -> None:
        """XAB (uppercase X) doesn't match disputed pattern (must start with lowercase x)."""
        assert is_valid_uppercase_id("XAB") is False

    def test_invalid_length_too_short(self) -> None:
        """US (2 chars) is not valid."""
        assert is_valid_uppercase_id("US") is False

    def test_invalid_length_too_long(self) -> None:
        """USAA (4 chars) is not valid."""
        assert is_valid_uppercase_id("USAA") is False

    def test_empty_string(self) -> None:
        """Empty string is not valid."""
        assert is_valid_uppercase_id("") is False


class TestIsoAlpha3CodesConstant:
    """Tests for the ISO_ALPHA3_CODES constant."""

    def test_contains_usa(self) -> None:
        """ISO codes include USA."""
        assert "USA" in ISO_ALPHA3_CODES

    def test_contains_gbr(self) -> None:
        """ISO codes include GBR."""
        assert "GBR" in ISO_ALPHA3_CODES

    def test_count_is_249(self) -> None:
        """ISO codes should have 249 entries (standard count)."""
        assert len(ISO_ALPHA3_CODES) == 249

    def test_all_uppercase(self) -> None:
        """All ISO codes should be uppercase."""
        for code in ISO_ALPHA3_CODES:
            assert code == code.upper(), f"Code {code} is not uppercase"

    def test_all_three_letters(self) -> None:
        """All ISO codes should be exactly 3 letters."""
        for code in ISO_ALPHA3_CODES:
            assert len(code) == 3, f"Code {code} is not 3 characters"
            assert code.isalpha(), f"Code {code} contains non-letter characters"


class TestHypothesisIsoCodePatterns:
    """Property-based tests for ISO code validation."""

    @given(st.sampled_from(list(ISO_ALPHA3_CODES)))
    @settings(max_examples=50)
    def test_all_iso_codes_are_valid(self, code: str) -> None:
        """All ISO 3166-1 alpha-3 codes should be valid."""
        assert is_valid_uppercase_id(code) is True

    @given(st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ", min_size=3, max_size=3))
    @settings(max_examples=100)
    def test_three_uppercase_either_iso_or_invalid(self, code: str) -> None:
        """Any 3 uppercase letters are either a valid ISO code or invalid."""
        result = is_valid_uppercase_id(code)
        if code in ISO_ALPHA3_CODES:
            assert result is True
        else:
            assert result is False

    @given(st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ", min_size=2, max_size=2))
    @settings(max_examples=50)
    def test_disputed_territory_pattern(self, suffix: str) -> None:
        """x + 2 uppercase letters is a valid disputed territory pattern."""
        disputed_code = f"x{suffix}"
        assert is_valid_uppercase_id(disputed_code) is True

    @given(st.text(alphabet="abcdefghijklmnopqrstuvwxyz", min_size=3, max_size=3))
    @settings(max_examples=50)
    def test_lowercase_codes_are_invalid(self, code: str) -> None:
        """All lowercase 3-letter codes should be invalid."""
        assert is_valid_uppercase_id(code) is False
