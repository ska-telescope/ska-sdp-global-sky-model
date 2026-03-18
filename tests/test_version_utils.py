"""
Tests for version_utils module.

This module tests semantic version parsing, validation, comparison,
and increment checking functions.
"""

from ska_sdp_global_sky_model.utilities.version_utils import (
    get_latest_version,
    increment_minor_version,
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


class TestIncrementMinorVersion:
    """Tests for increment_minor_version function."""

    def test_none_returns_initial_version(self):
        """When no existing version, return 0.1.0 for first upload."""
        assert increment_minor_version(None) == "0.1.0"

    def test_empty_string_returns_initial_version(self):
        """Empty string treated as no existing version."""
        assert increment_minor_version("") == "0.1.0"

    def test_increments_minor_from_zero(self):
        """0.1.0 increments to 0.2.0."""
        assert increment_minor_version("0.1.0") == "0.2.0"

    def test_increments_minor_sequential(self):
        """Successive minor increments: 0.2.0 -> 0.3.0."""
        assert increment_minor_version("0.2.0") == "0.3.0"

    def test_increments_minor_preserves_major(self):
        """Major version is preserved: 1.3.0 -> 1.4.0."""
        assert increment_minor_version("1.3.0") == "1.4.0"

    def test_increments_minor_preserves_patch(self):
        """Patch version is preserved: 1.2.5 -> 1.3.5."""
        assert increment_minor_version("1.2.5") == "1.3.5"

    def test_invalid_version_returns_initial(self):
        """Invalid version string falls back to 0.1.0."""
        assert increment_minor_version("invalid") == "0.1.0"

    def test_large_minor_version(self):
        """Large minor version incremented correctly: 0.99.0 -> 0.100.0."""
        assert increment_minor_version("0.99.0") == "0.100.0"
