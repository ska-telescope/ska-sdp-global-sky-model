"""
Gleam Catalog ingest
"""

# pylint: disable=R1708(stop-iteration-return)
# pylint: disable=E1101(no-member)
# pylint: disable=R0913(too-many-arguments)

import csv
import json
import logging
from itertools import zip_longest
from typing import Any, Dict, List, Optional

from astropy.coordinates import SkyCoord
from astropy_healpix import HEALPix
from astroquery.vizier import Vizier
from sqlalchemy import exc
from sqlalchemy.orm import Session

from ska_sdp_global_sky_model.api.app.model import (
    Band,
    NarrowBandData,
    SkyTile,
    Source,
    Telescope,
    WholeSky,
    WideBandData,
)
from ska_sdp_global_sky_model.configuration.config import NSIDE
from ska_sdp_global_sky_model.utilities.helper_functions import (
    calculate_percentage,
    convert_ra_dec_to_skycoord,
)

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
        self.file_location = file_location
        self.heading_missing = heading_missing or []
        self.heading_alias = heading_alias or {}
        with open(self.file_location, newline="", encoding="utf-8") as csvfile:
            self.len = sum(1 for row in csvfile)

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
            yield (
                SourceFile(
                    ingest_set["key"],
                    heading_alias=ingest_set["heading_alias"],
                    heading_missing=ingest_set["heading_missing"],
                ),
                ingest_set["bands"],
            )


def load_or_create_telescope(
    db: Session, catalog_config: dict, overwrite: bool = False
) -> Optional[Telescope]:
    """
    Loads a telescope by name from the database. If not found, creates a new one.

    Args:
        db: SQLAlchemy database object
        catalog_config: Dictionary of telescope configuration.

    Returns:
        Telescope object or None if not found and not created.
    """
    catalog_name = catalog_config["name"]
    logger.info("Creating new telescope: %s", catalog_name)
    try:
        telescope = db.query(Telescope).filter_by(name=catalog_name).first()
        if telescope:
            if overwrite:
                telescope.ingested = False
                db.commit()
        if not telescope:
            logger.info("Telescope does not exist ..")
            telescope = Telescope(
                name=catalog_name,
                frequency_min=catalog_config["frequency_min"],
                frequency_max=catalog_config["frequency_max"],
                ingested=False,
            )
            db.add(telescope)
            db.commit()
        else:
            logger.info("Telescope already exists, checking if catalog is ingested...")
            if telescope.ingested:
                logger.info("%s catalog already ingested, exiting.", catalog_name)
                return None
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Error loading telescope: %s", e)
        db.rollback()
        return None

    return telescope


def load_or_create_bands(
    db: Session, telescope_id: int, telescope_bands: list
) -> Dict[float, Band]:
    """Loads bands associated with the telescope from the database.

    If a band for a given center frequency is not found, a new Band object
    is created and added to the database.

    Args:
        db: An SQLAlchemy database session object.
        telescope_id: The ID of the telescope to retrieve bands for.
        telescope_bands: List of bands the data is registered over.

    Returns:
        A dictionary mapping center frequency (float) to Band objects.
    """
    logger.info("Loading bands associated with the telescope from the database...")
    bands = {}
    for band_cf in telescope_bands:
        logger.info("Loading band: %s", band_cf)
        band = db.query(Band).filter_by(centre=band_cf, telescope=telescope_id).first()
        if not band:
            band = Band(centre=band_cf, telescope=telescope_id)
            db.add(band)
            bands[band_cf] = band
        else:
            bands[band_cf] = band
    db.commit()
    return bands


def create_source_catalog_entry(
    db: Session, source: Dict[str, float], name: str, healpix: HEALPix, sky_map: WholeSky
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
    ipix = int(healpix.skycoord_to_healpix(sky_coord))
    hpx = ipix + 4 * NSIDE**2

    # check if tile housing source exists yet
    try:
        tile = db.query(SkyTile).filter(SkyTile.pk == hpx).one()
    except exc.NoResultFound:
        tile = SkyTile(hpx=hpx, pk=hpx)

    sky_map.tiles.append(tile)
    db.add_all([tile])

    db.commit()

    source_catalog = Source(
        name=name,
        Heal_Pix_Position=sky_coord,
        sky_coord=sky_coord,
        RAJ2000=source["RAJ2000"],
        RAJ2000_Error=source.get("e_RAJ2000"),
        DECJ2000=source["DEJ2000"],
        DECJ2000_Error=source.get("e_DEJ2000"),
        tile_id=hpx,
    )
    db.add(source_catalog)
    db.commit()
    return source_catalog


def create_wide_band_data_entry(
    db: Session, source: Dict[str, str] | SourceFile, source_catalog: Source, telescope: Telescope
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
        try:
            source_float[k] = float(source[k])
        except ValueError:
            source_float[k] = source[k]

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
    db: Session,
    source: Dict[str, str],
    source_catalog: Source,
    bands: Dict[float, Band],
    ingest_bands: list,
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
        ingest_bands: The list of bands to be ingested this run.

    Returns:
        None (the function does not return a meaningful value). If data conversion fails
        for any band, the loop terminates and None is returned.
    """
    for band_cf, band in bands.items():
        if band_cf not in ingest_bands:
            # Not processing this band from the current catalog source.
            continue
        band_id = band.id
        band_cf_str = str(band_cf)
        band_cf_str = f"0{band_cf_str}" if len(band_cf_str) < 3 else band_cf_str

        source_float = {}
        for k in source.keys():
            try:
                source_float[k] = float(source[k])
            except ValueError:
                source_float[k] = source[k]

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
    db: Session,
    source_data: List[Dict[str, float]] | SourceFile,
    bands: Dict[float, Band],
    telescope: Any,
    ingest_bands: list,
    catalog_config: dict,
    heal_pix: HEALPix,
    sky_map: WholeSky | None,
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
        ingest_bands: List of bands to ingest
        catalog_config: The catalog configuration.

    Returns:
        True if all source data entries are processed successfully, False otherwise.
    """

    logger.info("Processing source data...")

    count = 0
    num_source_data = len(source_data)
    for source in source_data:
        name = str(source.get(catalog_config["source"]))
        if count % 100 == 0:
            logger.info(
                "Loading source into database, progress: %s%%",
                str(calculate_percentage(dividend=count, divisor=num_source_data)),
            )
        count += 1

        source = db.query(Source).filter_by(name=name)
        if source is not None:  # Skip existing source
            continue

        source_catalog = create_source_catalog_entry(db, source, name, heal_pix, sky_map)
        if not source_catalog:
            # Error creating source catalog entry, data processing unsuccessful
            return False

        if wideband := catalog_config.get("ingest", {}).get("wideband"):
            if wideband is True:
                if not create_wide_band_data_entry(db, source, source_catalog, telescope):
                    # Error creating wide band entry, data processing unsuccessful
                    return False

        create_narrow_band_data_entry(db, source, source_catalog, bands, ingest_bands)

    return True


def get_full_catalog(db: Session, catalog_config, overwrite: bool = False) -> bool:
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

    hp = HEALPix(nside=NSIDE, order="nested", frame="icrs")

    # check if sky map exists yet
    try:
        sky_map = db.query(WholeSky).filter_by(id=1).one()
        logger.info("SKY MAP EXISTS")
    except exc.NoResultFound:
        sky_map = WholeSky(id=1, tiles=[])
        db.add(sky_map)
        db.commit()
        logger.info("SKY MAP DOES NOT EXIST")

    # 1. Load or create telescope
    telescope = load_or_create_telescope(db, catalog_config, overwrite)
    logger.info("Telescope loaded: %s", str(telescope))

    if not telescope:
        return False

    # 2. Get catalog data
    source_data = get_data_catalog_selector(catalog_config["ingest"])

    # 3. Load or create bands
    bands = load_or_create_bands(db, telescope.id, catalog_config["bands"])
    if not bands:
        return False

    for sources, ingest_bands in source_data:
        if not sources:
            logger.error("No data-sources found for %s", catalog_name)
            return False
        logger.info("Processing %s sources", str(len(sources)))
        # 4. Process source data
        if not process_source_data(
            db, sources, bands, telescope, ingest_bands, catalog_config, hp, sky_map
        ):
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
