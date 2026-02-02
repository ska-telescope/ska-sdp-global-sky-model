"""
Database-specific configuration for schema generation.

This file contains database-specific settings that are separate from the
data model definition. This includes:
- Database-specific additional fields
- Column name mappings for backward compatibility
- Table name overrides
- Custom serialization configurations
"""

from typing import Any

# Database-specific additional fields not present in the data model
# These are added for database optimization, indexing, or other database concerns
DB_SPECIFIC_FIELDS = {
    "SkySource": {
        "ra_error": {
            "type": "Float",
            "kwargs": {"nullable": True},
            "description": "Error in Right Ascension measurement",
        },
        "dec_error": {
            "type": "Float",
            "kwargs": {"nullable": True},
            "description": "Error in Declination measurement",
        },
        "healpix_index": {
            "type": "BigInteger",
            "kwargs": {"index": True, "nullable": False},
            "description": "HEALPix position for spatial indexing",
        },
    }
}

# Column name mappings - only override when absolutely necessary
# Maps dataclass field names to database column names
# If a field is not listed here, the field name is used directly
COLUMN_NAME_OVERRIDES = {
    # SkySource uses field names directly from the data model
    # No overrides needed - we match the data model exactly
    
    # GlobalSkyModelMetadata uses field names directly from the data model
    # No overrides needed - we match the data model exactly
}

# Table name overrides (if different from dataclass name)
TABLE_NAME_OVERRIDES = {
    "SkySource": "Source",
    # GlobalSkyModelMetadata uses its own name
}

# Fields that should have unique constraints
UNIQUE_FIELDS = {
    "SkySource": {"name"},
}

# Dataclasses that should not be generated as SQL models
SKIP_MODELS = {
    "GlobalSkyModel",  # Container class, not a table
}

# Custom serialization configuration for to_json methods
# Maps dataclass names to field mappings for JSON output
CUSTOM_JSON_SERIALIZATION = {
    "SkySource": {
        "name": "name",
        "coords": ("ra", "dec"),  # Tuple for coordinate pair in radians
        "healpix_index": "healpix_index",
        "i_pol": "i_pol",
        "major_ax": "major_ax",
        "minor_ax": "minor_ax",
        "pos_ang": "pos_ang",
        "spec_idx": "spec_idx",
        "log_spec_idx": "log_spec_idx",
        "spec_curv": "spec_curv",
        "polarization": {
            "q": "q_pol",
            "u": "u_pol",
            "v": "v_pol",
            "frac": "pol_frac",
            "ang": "pol_ang",
            "rot_meas": "rot_meas",
        },
    }
}


def get_column_name(class_name: str, field_name: str) -> str:
    """
    Get the database column name for a dataclass field.

    This function checks for overrides first, then uses the field name directly.

    Args:
        class_name: Name of the dataclass
        field_name: Name of the field

    Returns:
        Database column name
    """
    # Check for explicit override
    if class_name in COLUMN_NAME_OVERRIDES:
        if field_name in COLUMN_NAME_OVERRIDES[class_name]:
            return COLUMN_NAME_OVERRIDES[class_name][field_name]

    # Use the field name directly from the data model
    return field_name


def get_table_name(class_name: str) -> str:
    """
    Get the database table name for a dataclass.

    Args:
        class_name: Name of the dataclass

    Returns:
        Database table name
    """
    return TABLE_NAME_OVERRIDES.get(class_name, class_name)


def should_skip_model(class_name: str) -> bool:
    """
    Check if a dataclass should be skipped for SQL model generation.

    Args:
        class_name: Name of the dataclass

    Returns:
        True if the model should be skipped
    """
    return class_name in SKIP_MODELS


def get_additional_fields(class_name: str) -> dict[str, dict[str, Any]]:
    """
    Get database-specific additional fields for a model.

    Args:
        class_name: Name of the dataclass

    Returns:
        Dictionary of additional field configurations
    """
    return DB_SPECIFIC_FIELDS.get(class_name, {})


def get_unique_fields(class_name: str) -> set[str]:
    """
    Get fields that should have unique constraints.

    Args:
        class_name: Name of the dataclass

    Returns:
        Set of field names (database column names)
    """
    return UNIQUE_FIELDS.get(class_name, set())


def has_custom_json(class_name: str) -> bool:
    """
    Check if a model has custom JSON serialization.

    Args:
        class_name: Name of the dataclass

    Returns:
        True if custom JSON serialization is configured
    """
    return class_name in CUSTOM_JSON_SERIALIZATION


def get_json_mapping(class_name: str) -> dict[str, Any]:
    """
    Get the JSON field mapping for a model.

    Args:
        class_name: Name of the dataclass

    Returns:
        Dictionary mapping JSON keys to database column names
    """
    return CUSTOM_JSON_SERIALIZATION.get(class_name, {})
