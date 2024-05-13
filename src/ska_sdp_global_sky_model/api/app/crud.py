"""
CRUD functionality goes here.
"""

from sqlalchemy import and_, text
from sqlalchemy.orm import Session
from healpix_alchemy import Tile

from ska_sdp_global_sky_model.api.app.model import Source, AOI
from astropy.coordinates import SkyCoord

def get_pg_sphere_version(db: Session):
    """
    Requests version information from pg_sphere.
    """
    return db.execute(text("SELECT pg_sphere_version();"))


def get_local_sky_model(
    db,
    ra: list,
    dec: list,
    flux_wide: float,
    telescope: str,
    fov: float,
) -> dict:
    """
    Retrieves a local sky model from a global sky model for a given celestial observation.

    Args:
        paly: The bounds of the LSM
        flux_wide (float): Wide-field flux of the observation in Jy.
        telescope (str): Name of the telescope being used for the observation.
        fov (float): Field of view of the telescope in arcminutes.

    Returns:
        dict: A dictionary containing the local sky model information.

        The dictionary includes the following keys:
            - ra: The right ascension provided as input.
            - dec: The declination provided as input.
            - flux_wide: The wide-field flux provided as input.
            - telescope: The telescope name provided as input.
            - fov: The field of view provided as input.
            - local_data: ......
    """

    corners = SkyCoord(ra, dec, unit='deg')
    AOIs = [AOI(hpx=hpx) for hpx in Tile.tiles_from(corners)]
    [db.add(aoi) for aoi in AOIs]
    db.commit() # TODO: we need to clean these up later on again.
    aoi_ids = [aoi.id for aoi in AOIs]
    sources = db.query(
        Source
    ).filter(
        AOI.id.in_(aoi_ids),
        AOI.hpx.contains(Source.Heal_Pix_Position)
    )
    local_sky_model = {
        "region": {"ra": ra, "dec": dec},
        "sources": [source.to_json(db) for source in sources],
    }
    return local_sky_model


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
        filters.append(Source.DecJ2000 == dec)
    if flux_wide is not None:
        filters.append(Source.flux_wide == flux_wide)  # Replace with actual column name
    if telescope is not None:
        filters.append(Source.telescope == telescope)
    if fov is not None:
        filters.append(Source.fov == fov)  # Replace with actual column name

    # Combine filters using 'and_' if any filters are present
    if filters:
        query = query.filter(and_(*filters))

    return query.all()
