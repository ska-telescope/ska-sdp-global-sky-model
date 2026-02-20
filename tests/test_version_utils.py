"""
Tests for version_utils module.

This module tests semantic version parsing, validation, comparison,
and increment checking functions.
"""

import pytest

from ska_sdp_global_sky_model.utilities.version_utils import (
    compare_versions,
    get_latest_version,
    is_valid_semantic_version,
    is_version_increment,
    parse_semantic_version,
)


class TestParseSemanticVersion:
    """Tests for parse_semantic_version function."""

    def test_valid_version_basic(self):
        """Test parsing a basic valid semantic version."""
        result = parse_semantic_version("1.2.3")
        assert result == (1, 2, 3)

    def test_valid_version_zeros(self):
        """Test parsing version with zeros."""
        result = parse_semantic_version("0.0.0")
        assert result == (0, 0, 0)

    def test_valid_version_large_numbers(self):
        """Test parsing version with large numbers."""
        result = parse_semantic_version("10.20.30")
        assert result == (10, 20, 30)

    def test_invalid_version_too_few_parts(self):
        """Test parsing version with too few parts."""
        result = parse_semantic_version("1.2")
        assert result is None

    def test_invalid_version_too_many_parts(self):
        """Test parsing version with too many parts."""
        result = parse_semantic_version("1.2.3.4")
        assert result is None

    def test_invalid_version_non_numeric(self):
        """Test parsing version with non-numeric parts."""
        result = parse_semantic_version("1.a.3")
        assert result is None

    def test_invalid_version_negative(self):
        """Test parsing version with negative numbers."""
        result = parse_semantic_version("1.-2.3")
        assert result is None

    def test_invalid_version_empty(self):
        """Test parsing empty string."""
        result = parse_semantic_version("")
        assert result is None

    def test_invalid_version_whitespace(self):
        """Test parsing version with whitespace."""
        result = parse_semantic_version("1. 2.3")
        assert result is None


class TestIsValidSemanticVersion:
    """Tests for is_valid_semantic_version function."""

    def test_valid_version(self):
        """Test validation of valid semantic version."""
        assert is_valid_semantic_version("1.2.3") is True

    def test_valid_version_zeros(self):
        """Test validation of version with zeros."""
        assert is_valid_semantic_version("0.0.0") is True

    def test_valid_version_large_numbers(self):
        """Test validation with large numbers."""
        assert is_valid_semantic_version("100.200.300") is True

    def test_invalid_version_format(self):
        """Test validation of invalid format."""
        assert is_valid_semantic_version("1.2") is False

    def test_invalid_version_non_numeric(self):
        """Test validation of non-numeric version."""
        assert is_valid_semantic_version("1.a.3") is False

    def test_invalid_version_empty(self):
        """Test validation of empty string."""
        assert is_valid_semantic_version("") is False

    def test_invalid_version_none(self):
        """Test validation of None."""
        # None will cause TypeError in regex matching
        with pytest.raises(TypeError):
            is_valid_semantic_version(None)


class TestCompareVersions:
    """Tests for compare_versions function."""

    def test_equal_versions(self):
        """Test comparison of equal versions."""
        result = compare_versions("1.2.3", "1.2.3")
        assert result == 0

    def test_major_version_greater(self):
        """Test comparison when first version has greater major."""
        result = compare_versions("2.0.0", "1.9.9")
        assert result == 1

    def test_major_version_less(self):
        """Test comparison when first version has lesser major."""
        result = compare_versions("1.0.0", "2.0.0")
        assert result == -1

    def test_minor_version_greater(self):
        """Test comparison when first version has greater minor."""
        result = compare_versions("1.3.0", "1.2.9")
        assert result == 1

    def test_minor_version_less(self):
        """Test comparison when first version has lesser minor."""
        result = compare_versions("1.2.0", "1.3.0")
        assert result == -1

    def test_patch_version_greater(self):
        """Test comparison when first version has greater patch."""
        result = compare_versions("1.2.5", "1.2.3")
        assert result == 1

    def test_patch_version_less(self):
        """Test comparison when first version has lesser patch."""
        result = compare_versions("1.2.3", "1.2.5")
        assert result == -1

    def test_invalid_first_version(self):
        """Test comparison with invalid first version."""
        with pytest.raises(ValueError, match="Invalid semantic version"):
            compare_versions("invalid", "1.2.3")

    def test_invalid_second_version(self):
        """Test comparison with invalid second version."""
        with pytest.raises(ValueError, match="Invalid semantic version"):
            compare_versions("1.2.3", "invalid")

    def test_both_invalid_versions(self):
        """Test comparison with both versions invalid."""
        with pytest.raises(ValueError, match="Invalid semantic version"):
            compare_versions("invalid1", "invalid2")


class TestIsVersionIncrement:
    """Tests for is_version_increment function."""

    def test_first_version_empty_list(self):
        """Test validation with no existing versions."""
        valid, msg = is_version_increment("1.0.0", [])
        assert valid is True
        assert msg == ""

    def test_valid_increment_major(self):
        """Test valid major version increment."""
        valid, msg = is_version_increment("2.0.0", ["1.5.3", "1.2.0"])
        assert valid is True
        assert msg == ""

    def test_valid_increment_minor(self):
        """Test valid minor version increment."""
        valid, msg = is_version_increment("1.6.0", ["1.5.3", "1.2.0"])
        assert valid is True
        assert msg == ""

    def test_valid_increment_patch(self):
        """Test valid patch version increment."""
        valid, msg = is_version_increment("1.5.4", ["1.5.3", "1.2.0"])
        assert valid is True
        assert msg == ""

    def test_invalid_same_version(self):
        """Test invalid - same version already exists."""
        valid, msg = is_version_increment("1.5.3", ["1.5.3", "1.2.0"])
        assert valid is False
        assert "already exists" in msg.lower()
        assert "1.5.3" in msg

    def test_invalid_lower_version(self):
        """Test invalid - new version is lower than existing."""
        valid, msg = is_version_increment("1.4.0", ["1.5.3", "1.2.0"])
        assert valid is False
        assert "must be greater" in msg.lower()
        assert "1.5.3" in msg

    def test_invalid_format_new_version(self):
        """Test invalid new version format."""
        valid, msg = is_version_increment("invalid", ["1.5.3"])
        assert valid is False
        assert "invalid" in msg.lower() and "format" in msg.lower()

    def test_invalid_format_existing_version(self):
        """Test invalid format in existing versions list."""
        valid, msg = is_version_increment("2.0.0", ["1.5.3", "invalid"])
        assert valid is False
        assert "invalid" in msg.lower() or "comparison failed" in msg.lower()

    def test_multiple_existing_versions(self):
        """Test with multiple existing versions."""
        existing = ["1.0.0", "1.5.0", "2.0.0", "1.3.0"]
        valid, msg = is_version_increment("2.1.0", existing)
        assert valid is True
        assert msg == ""

    def test_edge_case_zero_version(self):
        """Test incrementing from 0.0.0."""
        valid, msg = is_version_increment("0.0.1", ["0.0.0"])
        assert valid is True
        assert msg == ""


class TestGetLatestVersion:
    """Tests for get_latest_version function."""

    def test_single_version(self):
        """Test with single version."""
        result = get_latest_version(["1.0.0"])
        assert result == "1.0.0"

    def test_multiple_versions_order(self):
        """Test with multiple versions in random order."""
        versions = ["1.5.3", "2.0.0", "1.2.0", "1.5.4"]
        result = get_latest_version(versions)
        assert result == "2.0.0"

    def test_patch_comparison(self):
        """Test comparison of patch versions."""
        versions = ["1.5.3", "1.5.10", "1.5.2"]
        result = get_latest_version(versions)
        assert result == "1.5.10"

    def test_empty_list(self):
        """Test with empty version list."""
        result = get_latest_version([])
        assert result is None

    def test_single_invalid_version(self):
        """Test with single invalid version."""
        result = get_latest_version(["invalid"])
        assert result is None

    def test_mixed_valid_invalid(self):
        """Test with mix of valid and invalid versions."""
        versions = ["1.0.0", "invalid", "2.0.0", "bad.version"]
        result = get_latest_version(versions)
        assert result == "2.0.0"

    def test_all_invalid_versions(self):
        """Test with all invalid versions."""
        versions = ["invalid1", "bad.version", "not.a.version"]
        result = get_latest_version(versions)
        assert result is None

    def test_major_version_priority(self):
        """Test that major version takes priority."""
        versions = ["1.99.99", "2.0.0", "1.100.100"]
        result = get_latest_version(versions)
        assert result == "2.0.0"

    def test_version_with_zeros(self):
        """Test comparison including 0.0.0."""
        versions = ["0.0.0", "0.1.0", "1.0.0"]
        result = get_latest_version(versions)
        assert result == "1.0.0"
