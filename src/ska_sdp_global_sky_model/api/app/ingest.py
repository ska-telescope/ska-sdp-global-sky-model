# pylint: disable=stop-iteration-return, no-member, too-many-positional-arguments
# pylint: disable=too-many-arguments, too-many-locals
"""
Gleam Catalog ingest
"""

import csv
import logging
import os
from itertools import zip_longest
from typing import Dict, Optional

import healpy as hp
import numpy as np
from astroquery.vizier import Vizier
from sqlalchemy.orm import Session

from ska_sdp_global_sky_model.api.app.models import SkyComponent, SkyComponentStaging
from ska_sdp_global_sky_model.configuration.config import NEST, NSIDE
from ska_sdp_global_sky_model.utilities.helper_functions import calculate_percentage

logger = logging.getLogger(__name__)


class SourceFile:
    """SourceFile cerates an iterator object which yields source dicts."""

    def __init__(
        self,
        file_location: str,
        heading_alias: dict | None = None,
        heading_missing: list | None = None,
    ):
        """Source file init method
        Args:
            file_location: A path to the file to be ingested.
            heading_alias: Alter headers to match our expected input.
            heading_missing: A list of headings to be padded onto the dataset
        """
        logger.info("Creating SourceFile object")
        self.file_location = file_location
        self.heading_missing = heading_missing or []
        self.heading_alias = heading_alias or {}

        # Get the file size in bytes
        file_size = os.path.getsize(self.file_location)

        # Print the file size
        logger.info("File size: %d bytes", file_size)
        try:
            with open(self.file_location, newline="", encoding="utf-8") as csvfile:
                logger.info("Opened file: %s", self.file_location)
                self.len = sum(1 for row in csvfile)
                logger.info("File length (rows): %s", self.len)

        except FileNotFoundError as f:
            logger.error("File not found: %s", self.file_location)
            self.len = 0
            raise RuntimeError from f
        except Exception as e:
            logger.error("Error opening file: %s", e)
            self.len = 0
            raise RuntimeError from e

        logger.info("SourceFile object created")

    def header(self, header) -> list:
        """Apply header aliasing
        Args:
            header: The header to be processed.
        """
        for item in self.heading_alias.items():
            for i, n in enumerate(header):
                if n == item[0]:
                    header[i] = item[1]
        return header + self.heading_missing

    def __iter__(self) -> iter:
        """Iterate through the sources"""
        logger.info("In the iterator opening %s", self.file_location)
        with open(self.file_location, newline="", encoding="utf-8") as csvfile:
            logger.info("opened")
            csv_file = csv.reader(csvfile, delimiter=",")
            heading = self.header(next(csv_file))
            for row in csv_file:
                yield dict(zip_longest(heading, row, fillvalue=None))

    def __len__(self) -> int:
        """Get the file length count."""
        return self.len


def get_data_catalog_vizier(key):
    """Get the catalog from vizier
    Args:
        key: The catalog key as per vizier.
    """
    Vizier.ROW_LIMIT = -1
    Vizier.columns = ["**"]
    catalog = Vizier.get_catalogs(key)
    return catalog[1]


def get_data_catalog_selector(ingest: dict):
    """Factory function to select the vizier vs file ingestor.
    Args:
        ingest: The catalog ingest configurations.
    """
    if ingest["agent"] == "vizier":
        yield get_data_catalog_vizier(ingest["key"]), ingest["bands"]
    elif ingest["agent"] == "file":
        for ingest_set in ingest["file_location"]:
            logger.info("Opening file: %s", ingest_set["key"])
            yield (
                SourceFile(
                    ingest_set["key"],
                    heading_alias=ingest_set["heading_alias"],
                    heading_missing=ingest_set["heading_missing"],
                ),
                ingest_set["bands"],
            )


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

    theta = np.radians(90.0 - dec_deg)
    phi = np.radians(ra_deg)
    return int(hp.ang2pix(nside, theta, phi, nest=nest))


def create_source_catalog_entry(
    db: Session,
    source: Dict[str, float],
    component_id: str,
    staging: bool = False,
    upload_id: Optional[str] = None,
) -> Optional[SkyComponent]:
    """Creates a SkyComponent object from the provided source data and adds it to the database.

    If any of the required keys (`RAJ2000`, `DEJ2000`) are missing from the source data,
    the function will return None.

    Args:
        db: An SQLAlchemy database session object.
        source: A dictionary containing the source information with the following keys:
            * `RAJ2000`: Right Ascension (J2000) in degrees (required).
            * `DEJ2000`: Declination (J2000) in degrees (required).
            * `CATALOG_NAME` (optional): Name of the component in the e.g. GLEAM catalog.
        component_id: String for the sky component_id.
        staging: If True, insert to staging table instead of main table.
        upload_id: Upload identifier for staging records.

    Returns:
        The created SkyComponent object, or None if required keys are missing from the data.
    """

    if staging:
        sky_component = SkyComponentStaging(
            component_id=component_id,
            healpix_index=compute_hpx_healpy(source["RAJ2000"], source["DEJ2000"]),
            ra=source["RAJ2000"],
            dec=source["DEJ2000"],
            upload_id=upload_id,
        )
    else:
        sky_component = SkyComponent(
            component_id=component_id,
            healpix_index=compute_hpx_healpy(source["RAJ2000"], source["DEJ2000"]),
            ra=source["RAJ2000"],
            dec=source["DEJ2000"],
        )
    db.add(sky_component)
    db.commit()
    return sky_component


def coerce_floats(source_dict: dict) -> dict:
    """Coerce values to floats."""
    out = {}
    for k, v in source_dict.items():
        try:
            out[k] = float(v)
        except (ValueError, TypeError):
            out[k] = v
    return out


def _add_optional_field(mapping: dict, source_dict: dict, field_name: str, *csv_aliases):
    """
    Add an optional field to the mapping if found in source_dict.

    Args:
        mapping: Target mapping dictionary
        source_dict: Source data dictionary
        field_name: Target field name
        csv_aliases: One or more CSV column names to check
    """
    for alias in csv_aliases:
        if alias in source_dict:
            value = to_float(source_dict.get(alias))
            if value is not None:
                mapping[field_name] = value
                return


def build_source_mapping(source_dict: dict, catalog_config: dict) -> dict:
    """
    Construct source structure mapping CSV columns to SkySource schema.

    Maps all available fields from the CSV to the SkySource dataclass fields.
    Required fields: component_id, ra, dec, i_pol
    Optional fields include source shape, spectral features, and polarization.

    Args:
        source_dict: Dictionary from CSV row with column names as keys
        catalog_config: Catalog configuration containing source name field

    Returns:
        Dictionary mapping to SkySource fields for database insertion
    """
    # Build base source mapping with required fields
    source_mapping = {
        "component_id": str(source_dict.get(catalog_config["source"])),
        "healpix_index": compute_hpx_healpy(source_dict["RAJ2000"], source_dict["DEJ2000"]),
        "ra": to_float(source_dict.get("RAJ2000")),
        "dec": to_float(source_dict.get("DEJ2000")),
        # I polarization - use wide-band or first available flux measurement
        "i_pol": to_float(
            source_dict.get("Fpwide") or source_dict.get("Fintwide") or source_dict.get("i_pol")
        ),
    }

    # Add optional source shape parameters (Gaussian model)
    _add_optional_field(source_mapping, source_dict, "major_ax", "major_ax", "awide")
    _add_optional_field(source_mapping, source_dict, "minor_ax", "minor_ax", "bwide")
    _add_optional_field(source_mapping, source_dict, "pos_ang", "pos_ang", "pawide")

    # Add spectral index as array if available
    if "alpha" in source_dict or "spec_idx" in source_dict:
        # Support either single alpha value or full spec_idx array
        alpha = to_float(source_dict.get("alpha"))
        if alpha is not None:
            source_mapping["spec_idx"] = [alpha, None, None, None, None]
        elif "spec_idx" in source_dict:
            source_mapping["spec_idx"] = source_dict.get("spec_idx")

    # Add spectral curvature if available
    _add_optional_field(source_mapping, source_dict, "spec_curv", "spec_curv")

    # Set log_spec_idx flag (default to None if not specified)
    if "log_spec_idx" in source_dict:
        log_val = source_dict.get("log_spec_idx")
        if isinstance(log_val, bool):
            source_mapping["log_spec_idx"] = log_val
        elif isinstance(log_val, str):
            source_mapping["log_spec_idx"] = log_val.lower() in ("true", "1", "yes")

    # Add Stokes polarization parameters
    _add_optional_field(source_mapping, source_dict, "q_pol", "q_pol")
    _add_optional_field(source_mapping, source_dict, "u_pol", "u_pol")
    _add_optional_field(source_mapping, source_dict, "v_pol", "v_pol")

    # Add polarization fraction and angle if available
    _add_optional_field(source_mapping, source_dict, "pol_frac", "pol_frac")
    _add_optional_field(source_mapping, source_dict, "pol_ang", "pol_ang")

    # Add rotation measure
    _add_optional_field(source_mapping, source_dict, "rot_meas", "rot_meas")

    return source_mapping


def validate_source_mapping(  # pylint: disable=too-many-return-statements
    source_mapping: dict, row_num: int = 0
) -> tuple[bool, Optional[str]]:
    """
    Validate a source mapping against the SkySource schema requirements.

    Checks that required fields are present and have valid types and values.
    This validation happens AFTER CSV transformation, ensuring data integrity
    before database insertion.

    Args:
        source_mapping: Dictionary with standardized SkySource fields
        row_num: Row number for error reporting (0 if unknown)

    Returns:
        (is_valid, error_message) - True if valid, False with error message otherwise
    """
    row_info = f" (row {row_num})" if row_num > 0 else ""

    # Check required fields exist
    required_fields = {"component_id": str, "ra": float, "dec": float, "i_pol": float}
    for field, expected_type in required_fields.items():
        if field not in source_mapping or source_mapping[field] is None:
            return False, f"Missing required field '{field}'{row_info}"

        # Type validation
        if not isinstance(source_mapping[field], expected_type):
            return (
                False,
                f"Field '{field}' has invalid type{row_info}: "
                f"expected {expected_type.__name__}, got {type(source_mapping[field]).__name__}",
            )

    # Validate RA range (radians: 0 to 2π, or degrees: 0 to 360)
    ra = source_mapping["ra"]
    if not -360 <= ra <= 360:  # Allow both radians and degrees
        return False, f"RA out of valid range{row_info}: {ra}"

    # Validate DEC range (radians: -π/2 to π/2, or degrees: -90 to 90)
    dec = source_mapping["dec"]
    if not -90 <= dec <= 90:  # Allow both radians and degrees
        return False, f"DEC out of valid range{row_info}: {dec}"

    # Validate i_pol is positive
    i_pol = source_mapping["i_pol"]
    if i_pol < 0:
        return False, f"i_pol (flux) must be positive{row_info}: {i_pol}"

    # Validate optional numeric fields if present
    optional_numeric = {
        "major_ax": (0, None),  # Must be positive
        "minor_ax": (0, None),  # Must be positive
        "pos_ang": (-180, 360),  # Position angle range
        "spec_curv": (None, None),  # No range restriction
        "q_pol": (None, None),  # Can be negative
        "u_pol": (None, None),  # Can be negative
        "v_pol": (None, None),  # Can be negative
        "pol_frac": (0, 1),  # Fraction 0-1
        "pol_ang": (0, 2 * 3.14159),  # Radians
        "rot_meas": (None, None),  # No range restriction
    }

    for field, (min_val, max_val) in optional_numeric.items():
        if field in source_mapping and source_mapping[field] is not None:
            value = source_mapping[field]
            if not isinstance(value, (int, float)):
                return (
                    False,
                    f"Field '{field}' must be numeric{row_info}: got {type(value).__name__}",
                )

            if min_val is not None and value < min_val:
                return False, f"Field '{field}' out of range{row_info}: {value} < {min_val}"

            if max_val is not None and value > max_val:
                return False, f"Field '{field}' out of range{row_info}: {value} > {max_val}"

    return True, None


def commit_batch(db: Session, component_objs: list, model_class=None):
    """Commit batches of sky components."""
    if not component_objs:
        return

    if model_class is None:
        model_class = SkyComponent
        
    db.bulk_insert_mappings(model_class, component_objs)
    db.commit()
    component_objs.clear()


def process_source_data_batch(
    db: Session,
    catalog_data,
    catalog_config,
    batch_size: int = 500,
    staging: bool = False,
    upload_id: Optional[str] = None,
) -> bool:
    """
    Processes source data and inserts into DB using batch operations for speed.

    Args:
        db: SQLAlchemy session.
        catalog_data: List of catalog source dictionaries.
        catalog_config: Catalog configuration.
        batch_size: Number of sources to insert per DB commit.
        staging: If True, insert to staging table.
        upload_id: Upload identifier for staging records.
    Returns:
        True if successful, False otherwise.
    """
    logger.info("Processing source data in batches (staging=%s)...", staging)

    # Choose appropriate model
    model_class = SkyComponentStaging if staging else SkyComponent
    
    existing_component_id = {r[0] for r in db.query(model_class.component_id).all()}

    component_objs = []

    count = 0
    total = len(catalog_data)
    validation_errors = 0

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
                "Progress: %s%%",
                calculate_percentage(count, total),
            )

        # Build the standardized source mapping
        source_mapping = build_source_mapping(source_dict, catalog_config)

        # Validate the source mapping before adding to batch
        is_valid, error_msg = validate_source_mapping(source_mapping, count)
        if not is_valid:
            logger.warning("Skipping invalid source %s: %s", component_id, error_msg)
            validation_errors += 1
            if validation_errors > 100:  # Stop if too many errors
                logger.error(
                    "Too many validation errors (%d), stopping ingestion", validation_errors
                )
                return False
            continue

        # Add upload_id for staging table
        if staging and upload_id:
            source_mapping["upload_id"] = upload_id
            
        component_objs.append(source_mapping)

        if count % batch_size == 0:
            commit_batch(db, component_objs, model_class)

    commit_batch(db, component_objs, model_class)

    if validation_errors > 0:
        logger.warning("Completed with %d validation errors (sources skipped)", validation_errors)

    return True


def get_full_catalog(db: Session, catalog_config) -> bool:
    """
    Downloads and processes a source catalog for a specified telescope.

    This function retrieves source data from the specified catalog and processes
    it into the schema, storing information in SkyComponent table (or staging table).

    Args:
        db: An SQLAlchemy database session object.
        catalog_config: Dictionary containing catalog configuration including:
            - name: Telescope name
            - catalog_name: Name of the catalog
            - ingest: Ingest configuration
            - source: Source name field
            - staging: (optional) If True, ingest to staging table
            - upload_id: (optional) Upload identifier for staging

    Returns:
        True if the catalog data is downloaded and processed successfully, False otherwise.
    """
    telescope_name = catalog_config["name"]
    catalog_name = catalog_config["catalog_name"]
    staging = catalog_config.get("staging", False)
    upload_id = catalog_config.get("upload_id")
    
    logger.info("Loading the %s catalog for the %s telescope (staging=%s)...", 
               catalog_name, telescope_name, staging)

    # Get catalog data
    catalog_data = get_data_catalog_selector(catalog_config["ingest"])

    for components, _ingest_bands in catalog_data:
        if not components:
            logger.error("No data-sources found for %s", catalog_name)
            return False
        logger.info("Processing %s components", str(len(components)))
        # Process source data
        if not process_source_data_batch(
            db,
            components,
            catalog_config,
            staging=staging,
            upload_id=upload_id,
        ):
            return False

    logger.info("Successfully ingested %s catalog", catalog_name)
    return True
