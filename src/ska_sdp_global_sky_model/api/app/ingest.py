# pylint: disable=stop-iteration-return, no-member, too-many-positional-arguments
# pylint: disable=too-many-arguments, too-many-locals
"""
Gleam Catalogue ingest
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

from ska_sdp_global_sky_model.api.app.models import SkyComponent, SkyComponentStaging
from ska_sdp_global_sky_model.configuration.config import NEST, NSIDE
from ska_sdp_global_sky_model.utilities.helper_functions import calculate_percentage

logger = logging.getLogger(__name__)


class ComponentFile:
    """ComponentFile creates an iterator object which yields component
    dicts from in-memory CSV text content."""

    def __init__(self, content: str):
        """Component file init method.

        Args:
            content: In-memory file content as string.
        """
        logger.debug("Creating ComponentFile object from in-memory content")
        self.content = content

        # Count lines from in-memory content
        self.len = content.count("\n")
        logger.debug("File length (rows) from memory: %s", self.len)
        logger.debug("ComponentFile object created")

    def __iter__(self) -> iter:
        """Iterate through the components from in-memory content."""
        logger.debug("Iterating over in-memory CSV content")
        csv_file = csv.reader(io.StringIO(self.content), delimiter=",")
        heading = next(csv_file)
        for row in csv_file:
            yield dict(zip_longest(heading, row, fillvalue=None))

    def __len__(self) -> int:
        """Get the file length count."""
        return self.len


def parse_catalogue_components(ingest: dict):
    """Parse catalogue components from in-memory CSV content.

    Converts the catalogue metadata containing file content into ComponentFile
    objects that can be iterated over for data ingestion.

    Args:
        ingest: Ingest metadata dictionary containing:
            - file_location: List of dicts with 'content' key (str)

    Yields:
        ComponentFile: Iterator object for each CSV file's content.

    Raises:
        ValueError: If content is missing from metadata.
    """
    for ingest_set in ingest["file_location"]:
        content = ingest_set.get("content")

        if not content:
            raise ValueError("Content (string) must be provided.")

        logger.debug("Processing in-memory content for catalogue ingestion")
        yield ComponentFile(content=content)


def to_float(val):
    """Convert value to float, returning None if conversion fails.

    Args:
        val: Value to convert (string, number, etc.).

    Returns:
        Float value, or None if conversion is not possible.
    """
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def compute_hpx_healpy(ra_deg, dec_deg, nside=NSIDE, nest=NEST):
    """Compute HEALPix index for given sky coordinates.

    Args:
        ra_deg: Right ascension in degrees.
        dec_deg: Declination in degrees.
        nside: HEALPix NSIDE parameter (default from config).
        nest: HEALPix ordering scheme (default from config).

    Returns:
        HEALPix pixel index as integer, or None if coordinates are invalid.
    """
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


def coerce_floats(component_dict: dict) -> dict:
    """Convert all numeric values in dictionary to floats where possible.

    Args:
        component_dict: Dictionary with string or numeric values.

    Returns:
        New dictionary with values converted to float where possible,
        original values preserved if conversion fails.
    """
    out = {}
    for k, v in component_dict.items():
        float_val = to_float(v)
        out[k] = float_val if float_val is not None else v
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


def _process_special_field(field_name: str, value, component_mapping: dict) -> bool:
    """
    Process special fields that need custom handling.

    Returns:
        True if field was processed, False otherwise
    """
    # Special handling for spec_idx (List type)
    if field_name == "spec_idx":
        if isinstance(value, str):
            float_val = to_float(value)
            component_mapping[field_name] = [float_val, None, None, None, None]
        elif isinstance(value, (int, float)):
            component_mapping[field_name] = [float(value), None, None, None, None]
        elif isinstance(value, list):
            component_mapping[field_name] = value
        else:
            # Field present but invalid type - store as None
            component_mapping[field_name] = [None, None, None, None, None]
        return True

    # Special handling for log_spec_idx (bool type)
    if field_name == "log_spec_idx":
        if isinstance(value, bool):
            component_mapping[field_name] = value
        elif isinstance(value, str):
            component_mapping[field_name] = value.lower() in ("true", "1", "yes")
        return True

    return False


def build_component_mapping(component_dict: dict) -> dict:
    """
    Construct component structure mapping CSV columns to SkyComponent schema dynamically.

    Automatically maps all fields from the SkyComponent dataclass, handling:
    - Required vs optional fields based on type annotations
    - Special conversions (arrays, booleans)
    - Database-specific fields (healpix_index)

    Args:
        component_dict: Dictionary from CSV row with column names as keys

    Returns:
        Dictionary mapping to SkyComponent fields for database insertion
    """
    component_mapping = {}
    dataclass_fields = _get_dataclass_fields()

    component_mapping["component_id"] = str(component_dict.get("component_id"))

    # Database-specific field (not in dataclass)
    component_mapping["healpix_index"] = compute_hpx_healpy(
        component_dict.get("ra"), component_dict.get("dec")
    )

    # Dynamically map all dataclass fields
    for field_name, field_type in dataclass_fields.items():
        if field_name == "component_id":
            continue  # Already handled above

        value = component_dict.get(field_name)
        if value is None:
            continue  # Skip missing optional fields

        # Handle special fields with custom processing
        if _process_special_field(field_name, value, component_mapping):
            continue

        # Standard numeric field conversion (skip list types)
        origin = get_origin(field_type)
        if origin is list:
            continue

        float_val = to_float(value)
        component_mapping[field_name] = float_val

    return component_mapping


def _validate_required_field(
    component_mapping: dict, field: str, expected_type: type, row_info: str
) -> tuple[bool, str | None]:
    """Validate a single required field exists and has correct type."""
    if field not in component_mapping or component_mapping[field] is None:
        return False, f"Missing required field '{field}'{row_info}"

    if not isinstance(component_mapping[field], expected_type):
        return (
            False,
            f"Field '{field}' has invalid type{row_info}: "
            f"expected {expected_type.__name__}, got {type(component_mapping[field]).__name__}",
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
    Define validation rules for fields that can be queried by the GSM.

    Only validates ranges for queryable coordinate fields.

    Returns:
        Dictionary mapping field names to (min_val, max_val) tuples
    """
    return {
        # Coordinates - queryable fields
        "ra": (0, 360),
        "dec": (-90, 90),
    }


def _validate_optional_numeric_fields(
    component_mapping: dict, row_info: str
) -> tuple[bool, str | None]:
    """Validate numeric fields based on dynamic validation rules."""
    validation_rules = _get_field_validation_rules()
    dataclass_fields = _get_dataclass_fields()

    for field_name, field_type in dataclass_fields.items():
        # Skip if field not in mapping
        if field_name not in component_mapping or component_mapping[field_name] is None:
            continue

        # Skip non-numeric types (lists, bools, strings)
        origin = get_origin(field_type)
        if origin is list or field_type is bool or field_type is str:
            continue

        # Get validation rule if exists
        if field_name in validation_rules:
            min_val, max_val = validation_rules[field_name]
            is_valid, error = _validate_numeric_range(
                component_mapping[field_name], field_name, min_val, max_val, row_info
            )
            if not is_valid:
                return False, error

    return True, None


def validate_component_mapping(
    component_mapping: dict, row_num: int = 0
) -> tuple[bool, str | None]:
    """
    Validate a component mapping against the SkyComponent schema requirements.

    Dynamically determines required fields from SkyComponent dataclass and
    validates types and value ranges.

    Args:
        component_mapping: Dictionary with standardized SkyComponent fields
        row_num: Row number for error reporting (0 if unknown)

    Returns:
        (is_valid, error_message) - True if valid, False with error message otherwise
    """
    required_fields = _get_required_fields()
    row_info = f" (row {row_num})" if row_num > 0 else ""

    # Validate required fields are present and have correct types
    for field_name, expected_type in required_fields.items():
        is_valid, error = _validate_required_field(
            component_mapping, field_name, expected_type, row_info
        )
        if not is_valid:
            return False, error

    # Validate optional numeric fields (includes coordinate and flux ranges)
    return _validate_optional_numeric_fields(component_mapping, row_info)


def commit_batch(db: Session, component_objs: list, model_class=SkyComponent):
    """Insert and commit a batch of sky components to the database.

    Uses bulk insert for efficiency and clears the list after commit.

    Args:
        db: SQLAlchemy database session.
        component_objs: List of component dictionaries to insert (modified in-place).
        model_class: SQLAlchemy model class (SkyComponent or SkyComponentStaging).
    """
    if not component_objs:
        return

    db.bulk_insert_mappings(model_class, component_objs)
    db.commit()
    component_objs.clear()


def _process_single_component(
    src,
    count: int,
    existing_component_id: set,
    staging: bool,
    upload_id: str | None,
) -> tuple[dict | None, str | None]:
    """Process and validate a single component.

    Args:
        src: Component data dictionary from CSV.
        count: Current row number for error reporting.
        existing_component_id: Set of already seen component IDs (modified in-place).
        staging: Whether ingesting to staging table.
        upload_id: Upload identifier for staging records.

    Returns:
        Tuple of (component_mapping, error_message). If valid, returns (mapping, None).
        If invalid, returns (None, error_message).
    """
    component_dict = dict(src) if not hasattr(src, "items") else src
    component_id = str(component_dict.get("component_id"))
    component_dict["component_id"] = component_id

    # Check for duplicate component_id
    if component_id in existing_component_id:
        error_msg = f"Row {count} (component_id: {component_id}): Duplicate component_id found"
        logger.warning("Validation error: %s", error_msg)
        return None, error_msg

    existing_component_id.add(component_id)

    # Build the standardized component mapping
    component_mapping = build_component_mapping(component_dict)

    # Add upload_id for staging records
    if staging and upload_id:
        component_mapping["upload_id"] = upload_id

    # Validate the component mapping
    is_valid, error_msg = validate_component_mapping(component_mapping, count)
    if not is_valid:
        return None, f"Row {count} (component_id: {component_id}): {error_msg}"

    return component_mapping, None


def process_component_data_batch(
    db: Session,
    catalogue_data,
    batch_size: int = 500,
    staging: bool = False,
    upload_id: str | None = None,
) -> bool:
    """
    Processes component data and inserts into DB using batch operations for speed.

    This function performs validation in two phases:
    1. Validate all components and collect errors
    2. Only ingest if all components are valid

    Args:
        db: SQLAlchemy session.
        catalogue_data: List of catalogue component dictionaries.
        catalogue_config: Catalogue configuration.
        batch_size: Number of components to insert per DB commit.
        staging: If True, insert to staging table.
        upload_id: Upload identifier for staging records.
    Returns:
        True if successful, False if validation errors occurred.
    """
    logger.info("Validating all component data before ingestion...")

    # Choose appropriate model
    model_class = SkyComponentStaging if staging else SkyComponent

    # For staging, only check duplicates within this batch (not global)
    # For main table, check all existing IDs
    if staging:
        # Empty set for staging - allow duplicates across uploads
        existing_component_id = set()
    else:
        existing_component_id = {r[0] for r in db.query(model_class.component_id).all()}

    # Phase 1: Validate all data and collect valid components
    component_objs = []
    validation_errors = []
    count = 0
    total = len(catalogue_data)

    for src in catalogue_data:
        count += 1

        if count % 100 == 0:
            logger.info(
                "Validation progress: %s%%",
                calculate_percentage(count, total),
            )

        component_mapping, error_msg = _process_single_component(
            src, count, existing_component_id, staging, upload_id
        )

        if error_msg:
            validation_errors.append(error_msg)
        else:
            component_objs.append(component_mapping)

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
        "All %d components validated successfully. Starting ingestion...", len(component_objs)
    )

    # Batch insert for efficiency
    total_to_ingest = len(component_objs)
    for i in range(0, total_to_ingest, batch_size):
        batch = component_objs[i : i + batch_size]  # noqa: E203
        commit_batch(db, batch, model_class)
        logger.info(
            "Ingestion progress: %s%%",
            calculate_percentage(min(i + batch_size, total_to_ingest), total_to_ingest),
        )

    logger.info("Successfully ingested %d components", total_to_ingest)
    return True


def ingest_catalogue(db: Session, catalogue_metadata) -> bool:
    """Ingest catalogue data from in-memory CSV content into the database.

    Processes component data from CSV content provided in the catalogue metadata,
    validates all records, and inserts them into the SkyComponent table.

    Args:
        db: SQLAlchemy database session.
        catalogue_metadata: Catalogue metadata dictionary containing:
            - name: Name used for logging messages
            - catalogue_name: Catalogue identifier for logging
            - ingest: Dictionary with 'file_location' key containing a list of
              dictionaries, each with a 'content' key holding CSV data as a string

    Returns:
        True if all data was validated and ingested successfully, False otherwise.
    """
    telescope_name = catalogue_metadata["name"]
    catalogue_name = catalogue_metadata["catalogue_name"]
    staging = catalogue_metadata.get("staging", False)
    upload_id = catalogue_metadata.get("upload_id")

    logger.info(
        "Loading the %s catalogue for the %s telescope (staging=%s)...",
        catalogue_name,
        telescope_name,
        staging,
    )

    # Parse catalogue components from metadata
    catalogue_data = parse_catalogue_components(catalogue_metadata["ingest"])

    for components in catalogue_data:
        if not components:
            logger.error("No data-components found for %s", catalogue_name)
            return False
        logger.info("Processing %s components", str(len(components)))
        # Process component data
        if not process_component_data_batch(
            db,
            components,
            staging=staging,
            upload_id=upload_id,
        ):
            return False

    logger.info("Successfully ingested %s catalogue", catalogue_name)
    return True
