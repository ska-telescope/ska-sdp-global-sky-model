"""
Utility functions for semantic version validation and comparison.
"""

import re


def parse_semantic_version(version: str) -> tuple[int, int, int] | None:
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


def increment_minor_version(version_str: str | None) -> str:
    """
    Increment the minor version of a semantic version string.

    Used to auto-assign a new version to each catalogue upload on a
    per-catalogue basis, without requiring the user to specify a version.

    - If no existing version (None or empty), starts at "0.1.0" for the first upload.
    - Otherwise increments the minor component: 0.1.0 -> 0.2.0 -> 0.3.0, etc.

    Args:
        version_str: Current latest version string in "major.minor.patch" format, or None.

    Returns:
        New version string with minor version incremented.
    """
    if not version_str:
        return "0.1.0"
    parsed = parse_semantic_version(version_str)
    if parsed is None:
        return "0.1.0"
    major, minor, patch = parsed
    return f"{major}.{minor + 1}.{patch}"


def get_latest_version(versions: list[str]) -> str | None:
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
