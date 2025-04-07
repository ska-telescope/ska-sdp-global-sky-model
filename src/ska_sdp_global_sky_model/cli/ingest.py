"""
Gleam Catalog ingest
"""

# pylint: disable=no-member

import logging
from itertools import zip_longest

import polars as pl
from astropy.coordinates import SkyCoord
from astropy_healpix import HEALPix
from astroquery.vizier import Vizier
from polars import DataFrame

from ska_sdp_global_sky_model.configuration.config import DATASET_ROOT, NSIDE, NSIDE_PIXEL
from ska_sdp_global_sky_model.configuration.datastore import DataStore, SourcePixel

logger = logging.getLogger(__name__)


def source_file(
    file_location: str, heading_alias: dict | None = None, heading_missing: list | None = None
):
    """Source file to DataFrame function
    Args:
        file_location: A path to the file to be ingested.
        heading_alias: Alter headers to match our expected input.
        heading_missing: A list of headings to be padded onto the dataset
    """
    logger.info("Creating SourceFile object")
    heading_missing = heading_missing or []
    heading_alias = heading_alias or {}

    path = DATASET_ROOT / file_location

    # Get the file size in bytes
    file_size = path.stat().st_size

    # Print the file size
    logger.info("File size: %d bytes", file_size)
    try:
        with path.open("r", newline="", encoding="utf-8") as csvfile:
            logger.info("Opened file: %s", path)
            file_len = sum(1 for row in csvfile)
            logger.info("File length (rows): %s", file_len)
        source_data = pl.read_csv(path)
    except FileNotFoundError as f:
        logger.error("File not found: %s", path)
        raise RuntimeError from f
    except Exception as e:
        logger.error("Error opening file: %s", e)
        raise RuntimeError from e
    logger.info("SourceFile object created")
    source_data = source_data.rename(heading_alias)
    source_data = source_data.with_columns(**dict(zip_longest(heading_missing, [])))
    # Get the HEALPix values for pixel and source resolution as well as SkyCoords vectors
    sc = SkyCoord(source_data["RAJ2000"], source_data["DEJ2000"], frame="icrs", unit="deg")
    healpix = HEALPix(nside=NSIDE, order="nested", frame="icrs")
    healpix_tile = HEALPix(nside=NSIDE_PIXEL, order="nested", frame="icrs")
    hp_source = healpix.skycoord_to_healpix(sc)
    hp_tile = healpix_tile.skycoord_to_healpix(sc)
    return source_data.with_columns(Heal_Pix_Position=hp_source, Heal_Pix_Tile=hp_tile)


def get_data_catalog_vizier(key):
    """Get the catalog from vizier
    Args:
        key: The catalog key as per vizier.
    """
    Vizier.ROW_LIMIT = -1
    Vizier.columns = ["**"]
    catalog = Vizier.get_catalogs(key)
    healpix = HEALPix(nside=NSIDE, order="nested", frame="icrs")
    healpix_tile = HEALPix(nside=NSIDE_PIXEL, order="nested", frame="icrs")
    tb = catalog[1]
    sc = SkyCoord(tb["RAJ2000"], tb["DEJ2000"], frame="icrs")
    hp_source = healpix.skycoord_to_healpix(sc)
    hp_tile = healpix_tile.skycoord_to_healpix(sc)
    return DataFrame(dict(tb.items())).with_columns(
        Heal_Pix_Position=hp_source, Heal_Pix_Tile=hp_tile
    )


def get_data_catalog_selector(ingest: dict):
    """Factory function to select the vizier vs file ingestor.
    Args:
        ingest: The catalog ingest configurations.
    """
    match ingest["agent"]:
        case "vizier":
            yield get_data_catalog_vizier(ingest["key"])
        case "file":
            for ingest_set in ingest["file_location"]:
                logger.info("Opening file: %s", ingest_set["key"])
                yield source_file(
                    ingest_set["key"],
                    heading_alias=ingest_set["heading_alias"],
                    heading_missing=ingest_set["heading_missing"],
                )
        case _:
            logger.error("Unknown agent type: '%s'", ingest["agent"])


def process_source_data(
    ds: DataStore,
    source_data: DataFrame,
    telescope: str,
    catalog_config: dict,
) -> bool:
    """Processes a list of source data entries and adds them to the database.

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
    source_data = source_data.rename({catalog_config["source"]: "name"})
    source_data = source_data.with_columns(pl.col("name").cast(pl.String))
    for tile in source_data["Heal_Pix_Tile"].unique().to_list():
        logger.info("Processing %s -> %s -> %s", catalog_config["name"], telescope, tile)
        source_tile = source_data.filter(Heal_Pix_Tile=tile)
        source_tile = source_tile.unique(subset=["name"], keep="first")
        if source_tile.is_empty():
            continue
        sp = SourcePixel(telescope, tile, ds.dataset_root)
        sp.read()
        sp.add(source_tile)
        sp.save()
        sp.clear()
        # free up memory
        del sp
    return True


def get_full_catalog(ds: DataStore, catalog_config: dict) -> bool:
    """
    Downloads and processes a source catalog for a specified telescope.

    The function logs informative messages during processing.

    Args:
        db: An SQLAlchemy database session object.

    Returns:
        True if the catalog data is downloaded and processed successfully, False otherwise.
    """
    telescope_name = catalog_config["name"]
    catalog_name = catalog_config["catalog_name"]
    logger.info("Loading the %s catalog for the %s telescope...", catalog_name, telescope_name)
    source_data = get_data_catalog_selector(catalog_config["ingest"])
    logger.info("Saving data...")
    for sources in source_data:
        if sources.is_empty():
            logger.error("No data-sources found for %s", catalog_name)
            return False
        logger.info("Processing %s sources", str(len(sources)))
        if not process_source_data(ds, sources, telescope_name, catalog_config):
            return False
    return True
