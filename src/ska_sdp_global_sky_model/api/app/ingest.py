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

from ska_sdp_global_sky_model.api.app.models import SkyComponent
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

    Returns:
        The created SkyComponent object, or None if required keys are missing from the data.
    """

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


def build_source_mapping(source_dict: dict, catalog_config: dict) -> dict:
    """Construct source structure."""
    return {
        "component_id": str(source_dict.get(catalog_config["source"])),
        "healpix_index": compute_hpx_healpy(source_dict["RAJ2000"], source_dict["DEJ2000"]),
        "ra": source_dict["RAJ2000"],
        "dec": source_dict["DEJ2000"],
    }


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

    Args:
        db: SQLAlchemy session.
        catalog_data: List of catalog source dictionaries.
        catalog_config: Catalog configuration.
        batch_size: Number of sources to insert per DB commit.
    Returns:
        True if successful, False otherwise.
    """
    logger.info("Processing source data in batches...")

    existing_component_id = {r[0] for r in db.query(SkyComponent.component_id).all()}

    component_objs = []

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
                "Progress: %s%%",
                calculate_percentage(count, total),
            )

        component_objs.append(build_source_mapping(source_dict, catalog_config))

        if count % batch_size == 0:
            commit_batch(db, component_objs)

    commit_batch(db, component_objs)

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
        ):
            return False

    logger.info("Successfully ingested %s catalog", catalog_name)
    return True
