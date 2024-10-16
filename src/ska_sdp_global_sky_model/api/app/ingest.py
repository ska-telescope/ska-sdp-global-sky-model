"""
Gleam Catalog ingest
"""

import csv
import json
import logging

from polars import DataFrame

# pylint: disable=R1708(stop-iteration-return)
# pylint: disable=E1101(no-member)
# pylint: disable=R0913(too-many-arguments)
import os
from itertools import zip_longest
from typing import Any, Dict, List, Optional

from astropy.coordinates import SkyCoord
from astropy_healpix import HEALPix
from astroquery.vizier import Vizier

from ska_sdp_global_sky_model.api.app.model import Source

from ska_sdp_global_sky_model.configuration.config import NSIDE, NSIDE_PIXEL
from ska_sdp_global_sky_model.utilities.helper_functions import (
    calculate_percentage,
    convert_ra_dec_to_skycoord,
)
from ska_sdp_global_sky_model.api.app.datastore import DataStore

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


def create_source_entry(
    ds: DataStore, source: Dict[str, float], name: str,
    band_names, telescope, wideband
) -> Optional[Source]:
    """Creates a Source object from the provided source data and adds it to the database.

    If any of the required keys (`RAJ2000`, `DEJ2000`) are missing from the source data,
    the function will return None.

    Args:
        db: An SQLAlchemy database session object.
        source: A dictionary containing the source information with the following keys:
            * `RAJ2000`: Right Ascension (J2000) in degrees (required).
            * `DEJ2000`: Declination (J2000) in degrees (required).
            * `CATALOG_NAME` (optional): Name of the source in the e.g. GLEAM catalog.
            * `e_RAJ2000` (optional): Uncertainty in Right Ascension (J2000) in degrees.
            * `e_DEJ2000` (optional): Uncertainty in Declination (J2000) in degrees.
        name: String for the source name.

    Returns:
        The created Source object, or None if required keys are missing from the data.
    """

    try:
        sky_coord: SkyCoord = convert_ra_dec_to_skycoord(source["RAJ2000"], source["DEJ2000"])
    except KeyError:
        # Required keys missing, return None
        logger.warning("Missing required keys in source data. Skipping source creation.")
        return None

    # get the HEALPix index of the coarse sky tile that would house it in the UNIQ pixel encoding
    healpix = HEALPix(nside=NSIDE, order="nested", frame="icrs")
    hp_source = int(healpix.skycoord_to_healpix(sky_coord))
    healpix_pixel = HEALPix(nside=NSIDE_PIXEL, order="nested", frame="icrs")
    hp_pixel = int(healpix_pixel.skycoord_to_healpix(sky_coord))


    source_float = {}
    for k in source.keys():
        if source[k] == None:
            source_float[k] = None
            continue
        try:
            source_float[k] = float(source[k])
        except ValueError:
            source_float[k] = source[k]

    source_dict = {
        # "source_id": source['source_id'],
        "name": name,
        "Heal_Pix_Position":hp_source,
        # "sky_coord":sky_coord,
        "RAJ2000":float(source["RAJ2000"]),
        "RAJ2000_Error":float(source.get("e_RAJ2000")),
        "DECJ2000":float(source["DEJ2000"]),
        "DECJ2000_Error":float(source.get("e_DEJ2000")),
    }
    if wideband:
        source_dict.update({
            "Bck_Wide":source_float["bckwide"],
            "Local_RMS_Wide":source_float["lrmswide"],
            "Int_Flux_Wide":source_float["Fintwide"],
            "Int_Flux_Wide_Error":source_float["e_Fintwide"],
            "Resid_Mean_Wide":source_float["resmwide"],
            "Resid_Sd_Wide":source_float["resstdwide"],
            "Abs_Flux_Pct_Error":source_float["e_Fpwide"],
            "Fit_Flux_Pct_Error":source_float["efitFpct"],
            "A_PSF_Wide":source_float["psfawide"],
            "B_PSF_Wide":source_float["psfbwide"],
            "PA_PSF_Wide":source_float["psfPAwide"],
            "Spectral_Index":source_float["alpha"],
            "Spectral_Index_Error":source_float["e_alpha"],
            "A_Wide":source_float["awide"],
            "A_Wide_Error":source_float["e_awide"],
            "B_Wide":source_float["bwide"],
            "B_Wide_Error":source_float["e_bwide"],
            "PA_Wide":source_float["pawide"],
            "PA_Wide_Error":source_float["e_pawide"],
            "Flux_Wide":source_float["Fpwide"],
            "Flux_Wide_Error":source_float["eabsFpct"],
        })

    for band_cf_str in band_names:
        source_dict.update({
            f"Bck_Narrow{band_cf_str}":source_float[f"bck{band_cf_str}"],
            f"Local_RMS_Narrow{band_cf_str}":source_float[f"lrms{band_cf_str}"],
            f"Int_Flux_Narrow{band_cf_str}":source_float[f"Fint{band_cf_str}"],
            f"Int_Flux_Narrow_Error{band_cf_str}":source_float[f"e_Fint{band_cf_str}"],
            f"Resid_Mean_Narrow{band_cf_str}":source_float[f"resm{band_cf_str}"],
            f"Resid_Sd_Narrow{band_cf_str}":source_float[f"resstd{band_cf_str}"],
            f"A_PSF_Narrow{band_cf_str}":source_float[f"psfa{band_cf_str}"],
            f"B_PSF_Narrow{band_cf_str}":source_float[f"psfb{band_cf_str}"],
            f"PA_PSF_Narrow{band_cf_str}":source_float[f"psfPA{band_cf_str}"],
            f"A_Narrow{band_cf_str}":source_float[f"a{band_cf_str}"],
            f"B_Narrow{band_cf_str}":source_float[f"b{band_cf_str}"],
            f"PA_Narrow{band_cf_str}":source_float[f"pa{band_cf_str}"],
            f"Flux_Narrow{band_cf_str}":source_float[f"Fp{band_cf_str}"],
            f"Flux_Narrow_Error{band_cf_str}":source_float[f"e_Fp{band_cf_str}"],
        })
    source_df = DataFrame(source_dict)
    ds.add_source(source_df, telescope, hp_pixel)


def get_bands(ingest_bands: list) -> str:
    """Creates NarrowBandData objects from the provided source data for each band and adds them to
    the database.

    This function expects the source data to have string values. It will attempt to convert
    them to floats before creating the NarrowBandData objects. If any conversion fails,
    the function will return None.

    Args:
        db: An SQLAlchemy database session object.
        source: A dictionary containing the narrow-band source information with string values.
        source_catalog: The corresponding Source object in the database.
        bands: A dictionary mapping center frequencies (floats) to Band objects.
        ingest_bands: The list of bands to be ingested this run.

    Returns:
        None (the function does not return a meaningful value). If data conversion fails
        for any band, the loop terminates and None is returned.
    """
    band_names = []
    for band_cf in ingest_bands:
        band_cf_str = str(band_cf)
        band_names.append(
            f"0{band_cf_str}" if len(band_cf_str) < 3 else band_cf_str)
    return band_names


def process_source_data(
    ds: DataStore,
    source_data: List[Dict[str, float]] | SourceFile,
    telescope: Any,
    ingest_bands: Any,
    catalog_config: dict,
) -> bool:
    """Processes a list of source data entries and adds them to the database.

    This function iterates over the provided source data and performs the following for each entry:

        1. Checks if a source with the same name (`GLEAM`) already exists in the database.
            - If it does, skip to the next entry.
        2. Creates a Source object using the `create_source_catalog_entry` function.
        3. Creates a WideBandData object using the `create_wide_band_data_entry` function.
        4. Creates NarrowBandData objects (one for each band) using the
           `create_narrow_band_data_entry` function.

    Args:
        ds: An Polars datastore session object.
        source_data: A list of dictionaries containing source information with float values.
        telescope: The telescope object (type can vary depending on your implementation).
        ingest_bands: List of bands to ingest
        catalog_config: The catalog configuration.

    Returns:
        True if all source data entries are processed successfully, False otherwise.
    """

    logger.info("Processing source data...")

    count = 0
    num_source_data = len(source_data)
    band_names = get_bands(ingest_bands)
    wideband = catalog_config.get("ingest", {}).get("wideband")
    for source in source_data:
        name = str(source.get(catalog_config["source"]))
        if count % 1000 == 0:
            logger.info(
                "Loading source into database, progress: %s%%",
                str(calculate_percentage(dividend=count, divisor=num_source_data)),
            )
        count += 1
        create_source_entry(ds, source, name, band_names, telescope, wideband)

    return True


def get_full_catalog(ds: DataStore, catalog_config) -> bool:
    """
    Downloads and processes a source catalog for a specified telescope.

    This function performs the following steps:

        1. Loads or creates a telescope record in the database based on the provided name.
        2. Retrieves source data for the specified catalog name.
        3. Loads or creates bands associated with the telescope.
        4. Processes the source data and adds the extracted information to the database.
        5. Updates the telescope record to indicate successful ingestion.

    The function logs informative messages during processing.

    Args:
        db: An SQLAlchemy database session object.

    Returns:
        True if the catalog data is downloaded and processed successfully, False otherwise.
    """
    telescope_name = catalog_config["name"]
    catalog_name = catalog_config["catalog_name"]
    logger.info("Loading the %s catalog for the %s telescope...", catalog_name, telescope_name)

    # 2. Get catalog data
    source_data = get_data_catalog_selector(catalog_config["ingest"])

    for sources, ingest_bands in source_data:
        if not sources:
            logger.error("No data-sources found for %s", catalog_name)
            return False
        logger.info("Processing %s sources", str(len(sources)))
        # 4. Process source data
        if not process_source_data(
            ds, sources, telescope_name, ingest_bands, catalog_config
        ):
            return False
    ds.save()
    return True

