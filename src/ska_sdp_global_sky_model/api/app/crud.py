# pylint: disable=no-member
# pylint: disable=too-many-locals
# pylint: disable=invalid-name
"""
CRUD functionality goes here.
"""

import logging
from collections import defaultdict

import astropy.units as u
from astropy.coordinates import Angle, Latitude, Longitude, SkyCoord
from astropy_healpix import HEALPix
from healpix_alchemy import Tile
from mocpy import MOC
from sqlalchemy import and_, or_
from sqlalchemy.orm import Session, aliased

from ska_sdp_global_sky_model.api.app.model import (
    Field,
    FieldTile,
    NarrowBandData,
    SkyTile,
    Source,
    WideBandData,
)
from ska_sdp_global_sky_model.configuration.config import NSIDE

logger = logging.getLogger(__name__)


def delete_previous_tiles(db):
    """
    Clear previous definitions of healpix tiles from the DB.
    """
    previous_fields = db.query(Field).first()
    db.delete(previous_fields)
    db.commit()


def get_precise_local_sky_model(db, ra, dec, fov):
    """
    Retrieves a local sky model (LSM) from a global sky model for a specific celestial observation.
    """
    moc = MOC.from_cone(
        lon=Longitude(float(ra[0]) * u.deg),
        lat=Latitude(float(dec[0]) * u.deg),
        radius=Angle(float(fov), u.deg),
        max_depth=10,
    )

    healpix_tiles = [FieldTile(hpx=hpx) for hpx in Tile.tiles_from(moc)]

    db.add(Field(tiles=healpix_tiles))
    db.commit()

    narrowband_data = aliased(NarrowBandData)
    wideband_data = aliased(WideBandData)

    sources_in_field = (
        db.query(Source, narrowband_data, wideband_data)
        .filter(
            or_(
                Source.Heal_Pix_Position.between(tile_range.hpx.lower, tile_range.hpx.upper)
                for tile_range in healpix_tiles
            )
        )
        .outerjoin(narrowband_data, Source.id == narrowband_data.source)
        .outerjoin(wideband_data, Source.id == wideband_data.source)
        .all()
    )

    results = {"sources": {}}

    for source in sources_in_field:

        source_dict = {
            "sky_coord": source.sky_coord,
        }

        if source.narrowband_data:
            source_dict["narrowband"] = source.narrowband_data.columns_to_dict()

        if source.wideband_data:
            source_dict["wideband"] = source.wideband_data.columns_to_dict()

        results["sources"][source.id] = source_dict

    logger.info(
        "Retrieve %s point sources within the area of interest.",
        str(len(sources_in_field)),
    )

    return results


def get_local_sky_model(
    db,
    ra: list,
    dec: list,
    flux_wide: float,
    _telescope: str,
    fov: float,
) -> dict:
    """
    Retrieves a local sky model (LSM) from a global sky model for a specific celestial observation.

    The LSM contains information about celestial sources within a designated region of the sky. \
        This function extracts this information from a database (`db`) based on the provided \
        right ascension (RA) and declination (Dec) coordinates.

    Args:
        db (Any): A database object containing the global sky model. The specific type of \
            database object will depend on the implementation.
        ra (list[float]): A list containing two right ascension values (in degrees) that define \
            the boundaries of the desired LSM region.
        dec (list[float]): A list containing two declination values (in degrees) that define the \
            boundaries of the desired LSM region.
        flux_wide (float): Placeholder for future implementation of wide-field flux \
            of the observation (in Jy). Currently not used.
        _telescope (str): Placeholder for future implementation of the telescope name \
            being used for the observation. Currently not used.
        fov (float): Placeholder for future implementation of the telescope's field of\
            view (in arcminutes). Currently not used.

    Returns:
        dict: A dictionary containing the LSM data. The structure of the dictionary is:

            {
                "region": {
                    "ra": List of RA coordinates (same as input `ra`).
                    "dec": List of Dec coordinates (same as input `dec`).
                },
                "count": Number of celestial sources found within the LSM region.
                "sources_in_area_of_interest": List of dictionaries, each representing a \
                    celestial source within the LSM region.
                    The structure of each source dictionary depends on the data model stored \
                        in the database (`db`).
            }
    """

    hp = HEALPix(nside=NSIDE, order="nested", frame="icrs")
    coord = SkyCoord(
        Longitude(float(ra[0]) * u.deg), Latitude(float(dec[0]) * u.deg), frame="icrs"
    )
    tiles = hp.cone_search_skycoord(coord, radius=float(fov) * u.deg)
    tiles += 4 * NSIDE**2
    tiles_int = getattr(tiles, "tolist", lambda: tiles)()

    # Aliases for narrowband and wideband data
    narrowband_data = aliased(NarrowBandData)
    wideband_data = aliased(WideBandData)

    # Modify the query to join the necessary tables
    query = (
        db.query(SkyTile, Source, narrowband_data, wideband_data)
        .filter(SkyTile.pk.in_(tiles_int))
        .filter(wideband_data.Flux_Wide > flux_wide)
        .join(SkyTile.sources)
        .outerjoin(narrowband_data, Source.id == narrowband_data.source)
        .outerjoin(wideband_data, Source.id == wideband_data.source)
        .all()
    )

    # Process the results into a structure
    results = defaultdict(
        lambda: {
            "sources": defaultdict(
                lambda: {"ra": None, "dec": None, "narrowband": [], "wideband": []}
            )
        }
    )

    for sky_tile, source, narrowband, wideband in query:
        source_data = results[sky_tile.pk]["sources"][source.id]
        source_data["ra"] = source.RAJ2000
        source_data["dec"] = source.DECJ2000
        if narrowband:
            source_data["narrowband"].append(narrowband.columns_to_dict())
        if wideband:
            source_data["wideband"].append(wideband.columns_to_dict())

    logger.info(
        "Retrieve %s point sources within the area of interest.",
        str(len(query)),
    )

    return results


def get_coverage_range(ra: float, dec: float, fov: float) -> tuple[float, float, float, float]:
    """
    This function calculates the minimum and maximum RA and Dec values
    covering a circular field of view around a given source position.

    Args:
        ra: Right Ascension of the source (in arcminutes)
        dec: Declination of the source (in arcminutes)
        fov: Diameter of the field of view (in arcminutes)

    Returns:
        A tuple containing (ra_min, ra_max, dec_min, dec_max)
    """

    # Input validation
    if fov <= 0:
        raise ValueError("Field of view must be a positive value.")
    if not 0 <= ra < 360:
        raise ValueError("Right Ascension (RA) must be between 0 and 360 degrees.")
    if not -90 <= dec <= 90:
        raise ValueError("Declination (Dec) must be between -90 and 90 degrees.")

    # Convert field of view diameter to radius
    fov_radius = fov / 2.0

    # Calculate RA range (assuming circular field)
    ra_min = ra - fov_radius
    ra_max = ra + fov_radius

    # Apply wrap-around logic for RA (0 to 360 degrees)
    ra_min = ra_min % 360.0
    ra_max = ra_max % 360.0

    # Calculate Dec range (assuming small field of view, no wrap-around)
    dec_min = dec - fov_radius
    dec_max = dec + fov_radius

    return ra_min, ra_max, dec_min, dec_max


# pylint: disable=too-many-arguments


def get_sources_by_criteria(
    db: Session,
    ra: float = None,
    dec: float = None,
    flux_wide: float = None,
    telescope: str = None,
    fov: float = None,
) -> list[Source]:
    """
    This function retrieves all Source entries matching the provided criteria.

    Args:
        db: A sqlalchemy database session object
        ra: Right Ascension (optional)
        dec: Declination (optional)
        flux_wide: Wideband flux (optional)
        telescope: Telescope name (optional)
        fov: Field of view (optional)

    Returns:
        A list of Source objects matching the criteria.
    """
    query = db.query(Source)

    # Build filter conditions based on provided arguments
    filters = []
    if ra is not None:
        filters.append(Source.RAJ2000 == ra)
    if dec is not None:
        filters.append(Source.DECJ2000 == dec)
    if flux_wide is not None:
        filters.append(Source.Flux_Wide == flux_wide)  # Replace with actual column name
    if telescope is not None:
        filters.append(Source.telescope == telescope)
    if fov is not None:
        filters.append(Source.fov == fov)  # Replace with actual column name

    # Combine filters using 'and_' if any filters are present
    if filters:
        query = query.filter(and_(*filters))

    return query.all()
