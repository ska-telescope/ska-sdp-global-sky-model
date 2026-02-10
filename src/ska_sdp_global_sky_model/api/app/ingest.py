# pylint: disable=stop-iteration-return, no-member, too-many-positional-arguments
# pylint: disable=too-many-arguments, too-many-locals
"""
Gleam Catalog ingest
"""

import csv
import dataclasses
import io
import logging
from itertools import zip_longest
from typing import get_args, get_origin

import healpy as hp
import numpy as np
from ska_sdp_datamodels.global_sky_model.global_sky_model import (
    SkyComponent as SkyComponentDataclass,
)
from sqlalchemy.orm import Session

from ska_sdp_global_sky_model.api.app.models import SkyComponent
from ska_sdp_global_sky_model.configuration.config import NEST, NSIDE
from ska_sdp_global_sky_model.utilities.helper_functions import calculate_percentage

logger = logging.getLogger(__name__)


class SourceFile:
    """SourceFile creates an iterator object which yields source dicts from in-memory content."""

    def __init__(self, content: bytes):
        """Source file init method.

        Args:
            content: In-memory file content as bytes.
        """
        logger.info("Creating SourceFile object from in-memory content")
        self.content = content

        # Count lines from in-memory content
        text_content = content.decode("utf-8")
        self.len = text_content.count("\n")
        logger.info("File length (rows) from memory: %s", self.len)
        logger.info("SourceFile object created")

    def __iter__(self) -> iter:
        """Iterate through the sources from in-memory content."""
        logger.info("Iterating over in-memory CSV content")
        text_content = self.content.decode("utf-8")
        csv_file = csv.reader(io.StringIO(text_content), delimiter=",")
        heading = next(csv_file)
        for row in csv_file:
            yield dict(zip_longest(heading, row, fillvalue=None))

    def __len__(self) -> int:
        """Get the file length count."""
        return self.len


def get_data_catalog_selector(ingest: dict):
    """Get in-memory catalog data sources for API bulk upload.

    Args:
        ingest: The catalog ingest configurations.

    Yields:
        SourceFile object for each content item in the configuration.
    """
    for ingest_set in ingest["file_location"]:
        content = ingest_set.get("content")

        if not content:
            raise ValueError("Content (bytes) must be provided.")

        logger.info("Processing in-memory content for API bulk upload")
        yield SourceFile(content=content)


def to_float(val):
    """Coerce to float."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def compute_hpx_healpy(ra_deg, dec_deg, nside=NSIDE, nest=NEST):
    """Computes the healpix position of a given source with particular NSIDE."""

    ra_deg = to_float(ra_deg)
    dec_deg = to_float(dec_deg)

    # Return None if coordinates are invalid (will be caught by validation)
    if ra_deg is None or dec_deg is None:
        return None

    try:
        theta = np.radians(90.0 - dec_deg)
        phi = np.radians(ra_deg)
        return int(hp.ang2pix(nside, theta, phi, nest=nest))
    except (ValueError, RuntimeError):
        # Invalid coordinates - will be caught by validation
        return None


def coerce_floats(source_dict: dict) -> dict:
    """Coerce values to floats."""
    out = {}
    for k, v in source_dict.items():
        try:
            out[k] = float(v)
        except (ValueError, TypeError):
            out[k] = v
    return out


def _is_optional_field(field_type: type) -> bool:
    """Check if a field type is Optional (Union[T, None])."""
    origin = get_origin(field_type)
    if origin is type(None | int):
        return True
    args = get_args(field_type)
    return origin is type(None) or (args and type(None) in args)


def _get_dataclass_fields() -> dict[str, type]:
    """
    Extract field names and types from SkyComponent dataclass.

    Returns:
        Dictionary mapping field names to their types
    """
    return dict(SkyComponentDataclass.__annotations__.items())


def _process_special_field(field_name: str, value, source_mapping: dict) -> bool:
    """
    Process special fields that need custom handling.

    Returns:
        True if field was processed, False otherwise
    """
    # Special handling for spec_idx (List type)
    if field_name == "spec_idx":
        if isinstance(value, str):
            float_val = to_float(value)
            if float_val is not None:
                source_mapping[field_name] = [float_val, None, None, None, None]
        elif isinstance(value, (int, float)):
            source_mapping[field_name] = [float(value), None, None, None, None]
        elif isinstance(value, list):
            source_mapping[field_name] = value
        return True

    # Special handling for log_spec_idx (bool type)
    if field_name == "log_spec_idx":
        if isinstance(value, bool):
            source_mapping[field_name] = value
        elif isinstance(value, str):
            source_mapping[field_name] = value.lower() in ("true", "1", "yes")
        return True

    return False


def build_source_mapping(source_dict: dict, catalog_config: dict) -> dict:
    """
    Construct source structure mapping CSV columns to SkySource schema dynamically.

    Automatically maps all fields from the SkyComponent dataclass, handling:
    - Required vs optional fields based on type annotations
    - Special conversions (arrays, booleans)
    - Database-specific fields (healpix_index)

    Args:
        source_dict: Dictionary from CSV row with column names as keys
        catalog_config: Catalog configuration containing source name field

    Returns:
        Dictionary mapping to SkySource fields for database insertion
    """
    source_mapping = {}
    dataclass_fields = _get_dataclass_fields()

    # Special handling for component_id (comes from config, not direct field name)
    source_mapping["component_id"] = str(source_dict.get(catalog_config["source"]))

    # Database-specific field (not in dataclass)
    source_mapping["healpix_index"] = compute_hpx_healpy(
        source_dict.get("ra"), source_dict.get("dec")
    )

    # Dynamically map all dataclass fields
    for field_name, field_type in dataclass_fields.items():
        if field_name == "component_id":
            continue  # Already handled above

        value = source_dict.get(field_name)
        if value is None:
            continue  # Skip missing optional fields

        # Handle special fields with custom processing
        if _process_special_field(field_name, value, source_mapping):
            continue

        # Standard numeric field conversion (skip list types)
        origin = get_origin(field_type)
        if origin is list:
            continue

        float_val = to_float(value)
        if float_val is not None:
            source_mapping[field_name] = float_val

    return source_mapping


def _validate_required_field(
    source_mapping: dict, field: str, expected_type: type, row_info: str
) -> tuple[bool, str | None]:
    """Validate a single required field exists and has correct type."""
    if field not in source_mapping or source_mapping[field] is None:
        return False, f"Missing required field '{field}'{row_info}"

    if not isinstance(source_mapping[field], expected_type):
        return (
            False,
            f"Field '{field}' has invalid type{row_info}: "
            f"expected {expected_type.__name__}, got {type(source_mapping[field]).__name__}",
        )
    return True, None


def _validate_numeric_range(
    value: float, field: str, min_val: float | None, max_val: float | None, row_info: str
) -> tuple[bool, str | None]:
    """Validate a numeric value is within specified range."""
    if not isinstance(value, (int, float)):
        return False, f"Field '{field}' must be numeric{row_info}: got {type(value).__name__}"

    if min_val is not None and value < min_val:
        return False, f"Field '{field}' out of range{row_info}: {value} < {min_val}"

    if max_val is not None and value > max_val:
        return False, f"Field '{field}' out of range{row_info}: {value} > {max_val}"

    return True, None


def _get_required_fields() -> dict[str, type]:
    """
    Determine required fields from SkyComponent dataclass.

    Fields without Optional and without default values are required.

    Returns:
        Dictionary of required field names to their types
    """
    required = {}
    for field_obj in dataclasses.fields(SkyComponentDataclass):
        field_name = field_obj.name
        field_type = field_obj.type

        # Skip if field has a default value or default_factory
        if (
            field_obj.default is not dataclasses.MISSING
            or field_obj.default_factory is not dataclasses.MISSING
        ):  # noqa: E501
            continue

        # Field is required - get its type
        if not _is_optional_field(field_type):
            # Extract the base type if it's wrapped
            origin = get_origin(field_type)
            if origin is list:
                required[field_name] = list
            else:
                required[field_name] = field_type

    return required


def _get_field_validation_rules() -> dict[str, tuple[float | None, float | None]]:
    """
    Define validation rules for numeric fields based on physical constraints.

    Returns:
        Dictionary mapping field names to (min_val, max_val) tuples
    """
    # These rules are based on physical/astronomical constraints
    return {
        # Source shape - must be positive
        "major_ax": (0, None),
        "minor_ax": (0, None),
        # Position angle - standard angular range
        "pos_ang": (-180, 360),
        # Spectral properties - no specific constraints
        "spec_curv": (None, None),
        # Stokes parameters - can be negative (signed quantities)
        "q_pol": (None, None),
        "u_pol": (None, None),
        "v_pol": (None, None),
        # Polarization fraction - 0 to 1 (fraction/percentage)
        "pol_frac": (0, 1),
        # Polarization angle - radians 0 to 2π
        "pol_ang": (0, 2 * 3.14159),
        # Rotation measure - no specific constraints
        "rot_meas": (None, None),
        # Coordinates
        "ra": (-360, 360),
        "dec": (-90, 90),
        # Flux must be positive
        "i_pol": (0, None),
    }


def _validate_optional_numeric_fields(
    source_mapping: dict, row_info: str
) -> tuple[bool, str | None]:
    """Validate numeric fields based on dynamic validation rules."""
    validation_rules = _get_field_validation_rules()
    dataclass_fields = _get_dataclass_fields()

    for field_name, field_type in dataclass_fields.items():
        # Skip if field not in mapping
        if field_name not in source_mapping or source_mapping[field_name] is None:
            continue

        # Skip non-numeric types (lists, bools, strings)
        origin = get_origin(field_type)
        if origin is list or field_type is bool or field_type is str:
            continue

        # Get validation rule if exists
        if field_name in validation_rules:
            min_val, max_val = validation_rules[field_name]
            is_valid, error = _validate_numeric_range(
                source_mapping[field_name], field_name, min_val, max_val, row_info
            )
            if not is_valid:
                return False, error

    return True, None


def validate_source_mapping(source_mapping: dict, row_num: int = 0) -> tuple[bool, str | None]:
    """
    Validate a source mapping against the SkySource schema requirements.

    Dynamically determines required fields from SkyComponent dataclass and
    validates types and value ranges.

    Args:
        source_mapping: Dictionary with standardized SkySource fields
        row_num: Row number for error reporting (0 if unknown)

    Returns:
        (is_valid, error_message) - True if valid, False with error message otherwise
    """
    required_fields = _get_required_fields()
    row_info = f" (row {row_num})" if row_num > 0 else ""

    # Validate required fields are present and have correct types
    for field_name, expected_type in required_fields.items():
        is_valid, error = _validate_required_field(
            source_mapping, field_name, expected_type, row_info
        )
        if not is_valid:
            return False, error

    # Validate optional numeric fields (includes coordinate and flux ranges)
    return _validate_optional_numeric_fields(source_mapping, row_info)


def commit_batch(db: Session, component_objs: list):
    """Commit batches of sky components."""
    if not component_objs:
        return

    db.bulk_insert_mappings(SkyComponent, component_objs)
    db.commit()
    component_objs.clear()


def process_source_data_batch(
    db: Session,
    catalog_data,
    catalog_config,
    batch_size: int = 500,
) -> bool:
    """
    Processes source data and inserts into DB using batch operations for speed.

    This function performs validation in two phases:
    1. Validate all sources and collect errors
    2. Only ingest if all sources are valid

    Args:
        db: SQLAlchemy session.
        catalog_data: List of catalog source dictionaries.
        catalog_config: Catalog configuration.
        batch_size: Number of sources to insert per DB commit.
    Returns:
        True if successful, False if validation errors occurred.
    """
    logger.info("Validating all source data before ingestion...")

    existing_component_id = {r[0] for r in db.query(SkyComponent.component_id).all()}

    # Phase 1: Validate all data and collect valid sources
    component_objs = []
    validation_errors = []
    count = 0
    total = len(catalog_data)

    for src in catalog_data:
        source_dict = dict(src) if not hasattr(src, "items") else src
        component_id = str(source_dict.get(catalog_config["source"]))

        if component_id in existing_component_id:
            continue
        existing_component_id.add(component_id)

        source_dict["component_id"] = component_id
        count += 1

        if count % 100 == 0:
            logger.info(
                "Validation progress: %s%%",
                calculate_percentage(count, total),
            )

        # Build the standardized source mapping
        source_mapping = build_source_mapping(source_dict, catalog_config)

        # Validate the source mapping
        is_valid, error_msg = validate_source_mapping(source_mapping, count)
        if not is_valid:
            error_entry = f"Row {count} (component_id: {component_id}): {error_msg}"
            validation_errors.append(error_entry)
            logger.warning("Validation error: %s", error_entry)
        else:
            component_objs.append(source_mapping)

    # Phase 2: Check if any validation errors occurred
    if validation_errors:
        logger.error(
            "Validation failed with %d errors. No data will be ingested.",
            len(validation_errors),
        )
        logger.error("All validation errors:")
        for error in validation_errors:
            logger.error("  - %s", error)
        return False

    # Phase 3: All validation passed, proceed with ingestion
    logger.info(
        "All %d sources validated successfully. Starting ingestion...", len(component_objs)
    )

    # Batch insert for efficiency
    total_to_ingest = len(component_objs)
    for i in range(0, total_to_ingest, batch_size):
        batch = component_objs[i : i + batch_size]  # noqa: E203
        commit_batch(db, batch)
        logger.info(
            "Ingestion progress: %s%%",
            calculate_percentage(min(i + batch_size, total_to_ingest), total_to_ingest),
        )

    logger.info("Successfully ingested %d sources", total_to_ingest)
    return True


def get_full_catalog(db: Session, catalog_config) -> bool:
    """
    Downloads and processes a source catalog for a specified telescope.

    This function retrieves source data from the specified catalog and processes
    it into the schema, storing all information directly in the SkyComponent table.

    Args:
        db: An SQLAlchemy database session object.
        catalog_config: Dictionary containing catalog configuration including:
            - name: Telescope name
            - catalog_name: Name of the catalog
            - ingest: Ingest configuration
            - source: Source name field

    Returns:
        True if the catalog data is downloaded and processed successfully, False otherwise.
    """
    telescope_name = catalog_config["name"]
    catalog_name = catalog_config["catalog_name"]
    logger.info("Loading the %s catalog for the %s telescope...", catalog_name, telescope_name)

    # Get catalog data
    catalog_data = get_data_catalog_selector(catalog_config["ingest"])

    for components in catalog_data:
        if not components:
            logger.error("No data-sources found for %s", catalog_name)
            return False
        logger.info("Processing %s components", str(len(components)))
        # Process source data
        if not process_source_data_batch(
            db,
            components,
            catalog_config,
        ):
            return False

    logger.info("Successfully ingested %s catalog", catalog_name)
    return True
