"""
Utility functions for semantic version validation and comparison.
"""

import re
from typing import Optional


def parse_semantic_version(version: str) -> Optional[tuple[int, int, int]]:
    """
    Parse a semantic version string into (major, minor, patch) tuple.

    Args:
        version: Version string in format "major.minor.patch"

    Returns:
        Tuple of (major, minor, patch) as integers, or None if invalid format
    """
    pattern = r"^(\d+)\.(\d+)\.(\d+)$"
    match = re.match(pattern, version)
    if not match:
        return None
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)))


def is_valid_semantic_version(version: str) -> bool:
    """
    Check if a version string follows semantic versioning format.

    Args:
        version: Version string to validate

    Returns:
        True if valid semantic version (major.minor.patch), False otherwise
    """
    return parse_semantic_version(version) is not None


def compare_versions(version1: str, version2: str) -> int:
    """
    Compare two semantic version strings.

    Args:
        version1: First version string
        version2: Second version string

    Returns:
        -1 if version1 < version2
         0 if version1 == version2
         1 if version1 > version2

    Raises:
        ValueError: If either version is invalid
    """
    v1 = parse_semantic_version(version1)
    v2 = parse_semantic_version(version2)

    if v1 is None:
        raise ValueError(f"Invalid semantic version: {version1}")
    if v2 is None:
        raise ValueError(f"Invalid semantic version: {version2}")

    if v1 < v2:
        return -1
    if v1 > v2:
        return 1
    return 0


def is_version_increment(new_version: str, existing_versions: list[str]) -> tuple[bool, str]:
    """
    Validate that new version is a proper increment over existing versions.

    A valid increment means the new version is greater than all existing versions.
    Empty existing_versions list means any valid semantic version is acceptable.

    Args:
        new_version: New version to validate
        existing_versions: List of existing version strings

    Returns:
        Tuple of (is_valid, error_message)
        - (True, "") if valid
        - (False, error_message) if invalid
    """
    # Validate new version format
    if not is_valid_semantic_version(new_version):
        return (
            False,
            f"Invalid version format: '{new_version}'. Must be semantic version (e.g., '1.0.0')",
        )

    # If no existing versions, any valid version is acceptable
    if not existing_versions:
        return (True, "")

    # Check new version against all existing versions
    try:
        for existing in existing_versions:
            comparison = compare_versions(new_version, existing)
            if comparison == 0:
                return (False, f"Version '{new_version}' already exists")
            if comparison < 0:
                return (
                    False,
                    f"Version '{new_version}' must be greater than existing version '{existing}'",
                )
    except ValueError as e:
        return (False, f"Version comparison failed: {str(e)}")

    return (True, "")


def get_latest_version(versions: list[str]) -> Optional[str]:
    """
    Get the latest (highest) version from a list of version strings.

    Args:
        versions: List of semantic version strings

    Returns:
        Latest version string, or None if list is empty or contains invalid versions
    """
    if not versions:
        return None

    valid_versions = []
    for v in versions:
        parsed = parse_semantic_version(v)
        if parsed:
            valid_versions.append((parsed, v))

    if not valid_versions:
        return None

    # Sort by parsed tuple and return the original string of the highest
    valid_versions.sort(key=lambda x: x[0], reverse=True)
    return valid_versions[0][1]
