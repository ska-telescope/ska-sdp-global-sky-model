"""
Gleam Catalog ingest
"""

import json
import logging
from typing import Any, Dict, List, Optional, Union

from astroquery.vizier import Vizier
from sqlalchemy.orm import Session

from ska_sdp_global_sky_model.api.app.model import (
    Band,
    NarrowBandData,
    Source,
    Telescope,
    WideBandData,
)
from ska_sdp_global_sky_model.utilities.helper_functions import (
    calculate_percentage,
    convert_ra_dec_to_skycoord,
)

logger = logging.getLogger(__name__)


def load_or_create_telescope(db: Session, telescope_name: str) -> Optional[Telescope]:
    """
    Loads a telescope by name from the database. If not found, creates a new one.

    Args:
        db: SQLAlchemy database object
        telescope_name: Name of the telescope to load

    Returns:
        Telescope object or None if not found and not created.
    """

    telescope = db.query(Telescope).filter_by(name=telescope_name).first()
    if not telescope:
        telescope = Telescope(
            name=telescope_name,
            frequency_min=80,
            frequency_max=300,
            ingested=False,
        )
        db.add(telescope)
        db.commit()
    else:
        if telescope.ingested:
            logger.info("Gleam catalog already ingested, exiting.")
            return None
    return telescope


def get_catalog_data(catalog_name: str) -> Union[None, List]:
    """
    Fetches the catalog data from Vizier for the given catalog name.

    This function retrieves catalog data from the Vizier service, respecting
    a maximum row limit.

    Args:
        catalog_name (str): Name of the catalog to fetch.

    Returns:
        Union[None, List]: The downloaded catalog data as a list of rows,
                           or None if the catalog is not found.

    Raises:
        AttributeError: If the `vizier` library is not installed or does
                        not have the expected attributes (e.g., `ROW_LIMIT`,
                        `columns`).
    """

    try:
        Vizier.ROW_LIMIT = -1
        Vizier.columns = ["**"]  # All columns

        logger.info("Loading the catalog %s from Vizier", catalog_name)

        catalog = Vizier.get_catalogs(catalog_name)

        if catalog:
            return catalog[1]
        logger.warning("Catalog %s not found on Vizier", catalog_name)
        return None

    except AttributeError as exception:
        logger.error("Error fetching catalog data: %s", exception)
        return None


def load_or_create_bands(db: Session, telescope_id: int) -> Dict[float, Band]:
    """Loads bands associated with the telescope from the database.

    If a band for a given center frequency is not found, a new Band object
    is created and added to the database.

    Args:
        db: An SQLAlchemy database session object.
        telescope_id: The ID of the telescope to retrieve bands for.

    Returns:
        A dictionary mapping center frequency (float) to Band objects.
    """

    logger.info("Loading bands associated with the telescope from the database...")
    band_frequencies = [
        76,
        84,
        92,
        99,
        107,
        115,
        122,
        130,
        143,
        151,
        158,
        166,
        174,
        181,
        189,
        197,
        204,
        212,
        220,
        227,
    ]
    bands = {}
    for band_cf in band_frequencies:
        logger.info("Loading band: %s", band_cf)
        band = db.query(Band).filter_by(centre=band_cf, telescope=telescope_id).first()
        if not band:
            band = Band(centre=band_cf, telescope=telescope_id)
            db.add(band)
            bands[band_cf] = band
    db.commit()
    return bands


def create_source_catalog_entry(db: Session, source: Dict[str, float]) -> Optional[Source]:
    """Creates a Source object from the provided source data and adds it to the database.

    If any of the required keys (`RAJ2000`, `DEJ2000`) are missing from the source data,
    the function will return None.

    Args:
        db: An SQLAlchemy database session object.
        source: A dictionary containing the source information with the following keys:
            * `RAJ2000`: Right Ascension (J2000) in degrees (required).
            * `DEJ2000`: Declination (J2000) in degrees (required).
            * `GLEAM` (optional): Name of the source in the GLEAM catalog.
            * `e_RAJ2000` (optional): Uncertainty in Right Ascension (J2000) in degrees.
            * `e_DEJ2000` (optional): Uncertainty in Declination (J2000) in degrees.

    Returns:
        The created Source object, or None if required keys are missing from the data.
    """

    try:
        point = convert_ra_dec_to_skycoord(source["RAJ2000"], source["DEJ2000"])
    except KeyError:
        # Required keys missing, return None
        logger.warning("Missing required keys in source data. Skipping source creation.")
        return None

    source_catalog = Source(
        name=source.get("GLEAM"),
        Heal_Pix_Position=point,
        RAJ2000=source["RAJ2000"],
        RAJ2000_Error=source.get("e_RAJ2000"),
        DECJ2000=source["DEJ2000"],
        DECJ2000_Error=source.get("e_DEJ2000"),
    )
    db.add(source_catalog)
    db.commit()
    return source_catalog


def create_wide_band_data_entry(
    db: Session, source: Dict[str, str], source_catalog: Source, telescope: Any
) -> Optional[WideBandData]:
    """Creates a WideBandData object from the provided source data and adds it to the database.

    This function expects the source data to have string values. It will attempt to convert
    them to floats before creating the WideBandData object. If any conversion fails, the
    function will return None.

    Args:
        db: An SQLAlchemy database session object.
        source: A dictionary containing the wide-band source information with string values.
        source_catalog: The corresponding Source object in the database.
        telescope: The telescope object (type can vary depending on your implementation).

    Returns:
        The created WideBandData object, or None if data conversion fails.
    """
    source_float = {}
    for k in source.keys():
        if k == "GLEAM":
            pass
        else:
            source_float[k] = float(source[k])

    wide_band_data = WideBandData(
        Bck_Wide=source_float["bckwide"],
        Local_RMS_Wide=source_float["lrmswide"],
        Int_Flux_Wide=source_float["Fintwide"],
        Int_Flux_Wide_Error=source_float["e_Fintwide"],
        Resid_Mean_Wide=source_float["resmwide"],
        Resid_Sd_Wide=source_float["resstdwide"],
        Abs_Flux_Pct_Error=source_float["e_Fpwide"],
        Fit_Flux_Pct_Error=source_float["efitFpct"],
        A_PSF_Wide=source_float["psfawide"],
        B_PSF_Wide=source_float["psfbwide"],
        PA_PSF_Wide=source_float["psfPAwide"],
        Spectral_Index=source_float["alpha"],
        Spectral_Index_Error=source_float["e_alpha"],
        A_Wide=source_float["awide"],
        A_Wide_Error=source_float["e_awide"],
        B_Wide=source_float["bwide"],
        B_Wide_Error=source_float["e_bwide"],
        PA_Wide=source_float["pawide"],
        PA_Wide_Error=source_float["e_pawide"],
        Flux_Wide=source_float["Fpwide"],
        Flux_Wide_Error=source_float["eabsFpct"],
        telescope=telescope.id,
        source=source_catalog.id,
    )
    db.add(wide_band_data)
    db.commit()
    return wide_band_data


def create_narrow_band_data_entry(
    db: Session, source: Dict[str, str], source_catalog: Source, bands: Dict[float, Band]
) -> Optional[None]:
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

    Returns:
        None (the function does not return a meaningful value). If data conversion fails
        for any band, the loop terminates and None is returned.
    """
    for band_cf, band in bands.items():
        band_id = band.id
        band_cf_str = ("0" + str(band_cf))[-3:]

        source_float = {}
        for k in source.keys():
            if k == "GLEAM":
                pass
            else:
                source_float[k] = float(source[k])

        narrow_band_data = NarrowBandData(
            Bck_Narrow=source_float[f"bck{band_cf_str}"],
            Local_RMS_Narrow=source_float[f"lrms{band_cf_str}"],
            Int_Flux_Narrow=source_float[f"Fint{band_cf_str}"],
            Int_Flux_Narrow_Error=source_float[f"e_Fint{band_cf_str}"],
            Resid_Mean_Narrow=source_float[f"resm{band_cf_str}"],
            Resid_Sd_Narrow=source_float[f"resstd{band_cf_str}"],
            A_PSF_Narrow=source_float[f"psfa{band_cf_str}"],
            B_PSF_Narrow=source_float[f"psfb{band_cf_str}"],
            PA_PSF_Narrow=source_float[f"psfPA{band_cf_str}"],
            A_Narrow=source_float[f"a{band_cf_str}"],
            B_Narrow=source_float[f"b{band_cf_str}"],
            PA_Narrow=source_float[f"pa{band_cf_str}"],
            Flux_Narrow=source_float[f"Fp{band_cf_str}"],
            Flux_Narrow_Error=source_float[f"e_Fp{band_cf_str}"],
            source=source_catalog.id,
            band=band_id,
        )
        db.add(narrow_band_data)
        db.commit()


def process_source_data(
    db: Session, source_data: List[Dict[str, float]], bands: Dict[float, Band], telescope: Any
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
        db: An SQLAlchemy database session object.
        source_data: A list of dictionaries containing source information with float values.
        bands: A dictionary mapping center frequencies (floats) to Band objects.
        telescope: The telescope object (type can vary depending on your implementation).

    Returns:
        True if all source data entries are processed successfully, False otherwise.
    """

    logger.info("Processing source data...")

    count = 0
    num_source_data = len(source_data)
    for source in source_data:
        name = source["GLEAM"]
        if count % 100 == 0:
            logger.info(
                "Loading source into database, progress: %s%%",
                str(calculate_percentage(dividend=count, divisor=num_source_data)),
            )
        count += 1

        if db.query(Source).filter_by(name=name).count():
            # Skip existing source
            continue

        source_catalog = create_source_catalog_entry(db, source)
        if not source_catalog:
            # Error creating source catalog entry, data processing unsuccessful
            return False

        if not create_wide_band_data_entry(db, source, source_catalog, telescope):
            # Error creating wide band entry, data processing unsuccessful
            return False

        create_narrow_band_data_entry(db, source, source_catalog, bands)

    return True


def get_full_catalog(db: Session) -> bool:
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

    catalog_name = "VIII/100"
    telescope_name = "Murchison Widefield Array"
    logger.info("Loading the %s catalog for the %s telescope...", catalog_name, telescope_name)

    # 1. Load or create telescope
    telescope = load_or_create_telescope(db, telescope_name)
    if not telescope:
        return False

    # 2. Get catalog data
    source_data = get_catalog_data(catalog_name)
    if not source_data:
        return False

    # 3. Load or create bands
    bands = load_or_create_bands(db, telescope.id)
    if not bands:
        return False

    # 4. Process source data
    if not process_source_data(db, source_data, bands, telescope):
        return False

    # 5. Mark telescope as ingested
    telescope.ingested = True
    db.add(telescope)
    db.commit()

    return True


def post_process(db):
    """Not currently used, but the intent is to pre-create the json field in the sources table"""
    count = 0
    for source in db.query(Source).all():
        logger.info("Loading source json: %s", str(count))
        source.json = json.dumps(source.to_json(db))
        db.add(source)
        count += 1
        if count % 100 == 0:
            db.commit()
    db.commit()
    return db.query(Source).all().count()
