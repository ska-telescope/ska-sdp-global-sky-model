# pylint: disable=no-member, too-many-locals, invalid-name,
# pylint: disable=too-many-positional-arguments, too-few-public-methods
"""
CRUD functionality goes here.
"""

import logging

from sqlalchemy import and_
from sqlalchemy.orm import Session
from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.types import Boolean

from ska_sdp_global_sky_model.api.app.models import SkyComponent

logger = logging.getLogger(__name__)


# pylint: disable=abstract-method,too-many-ancestors
class q3c_radial_query(GenericFunction):
    """SQLAlchemy function for h3c_radial_query(hpx, center, radius) -> BOOLEAN"""

    type = Boolean()
    inherit_cache = True
    name = "q3c_radial_query"


def get_local_sky_model(
    db,
    ra: float,
    dec: float,
    flux_wide: float,
    _telescope: str,
    fov: float,
) -> dict:
    """
    Retrieves a local sky model (LSM) from a global sky model for a specific celestial observation.

    The LSM contains information about celestial sources within a designated region of the sky.
    This function extracts this information from a database based on the provided
    right ascension (RA) and declination (Dec) coordinates.

    Args:
        db (Any): A database object containing the global sky model.
        ra (list[float]): A list containing right ascension values (in degrees).
        dec (list[float]): A list containing declination values (in degrees).
        flux_wide (float): Minimum wide-field flux threshold (in Jy).
        _telescope (str): Telescope name (currently unused in simplified model).
        fov (float): Field of view (in arcminutes).

    Returns:
        dict: A dictionary containing the LSM data with structure:
            {
                "components": {
                    component_id: {
                        "ra": float,
                        "dec": float,
                        "i_pol": float,
                        "major_ax": float,
                        "minor_ax": float,
                        ...
                    }
                }
            }
    """

    # Query sky components within field of view
    components = (
        db.query(SkyComponent)
        .where(
            q3c_radial_query(
                SkyComponent.ra,
                SkyComponent.dec,
                float(ra[0]),
                float(dec[0]),
                float(fov),
            )
        )
        .filter(SkyComponent.i_pol > flux_wide)
        .all()
    )

    # Return if no components were found
    if not components:
        return {"components": {}}

    # Build results using the SkyComponent model
    results = {"components": {}}

    for component in components:
        results["components"][component.component_id] = {
            "ra": component.ra,
            "dec": component.dec,
            "i_pol": component.i_pol,
            "major_ax": component.major_ax,
            "minor_ax": component.minor_ax,
            "pos_ang": component.pos_ang,
            "spec_idx": component.spec_idx,
            "q_pol": component.q_pol,
            "u_pol": component.u_pol,
            "v_pol": component.v_pol,
        }

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


def get_sky_components_by_criteria(
    db: Session,
    ra: float = None,
    dec: float = None,
    flux_wide: float = None,
) -> list[SkyComponent]:
    """
    This function retrieves all SkyComponent entries matching the provided criteria.

    Args:
        db: A sqlalchemy database session object
        ra: Right Ascension (optional)
        dec: Declination (optional)
        flux_wide: Minimum I polarization flux (optional)

    Returns:
        A list of SkyComponent objects matching the criteria.
    """
    query = db.query(SkyComponent)

    # Build filter conditions based on provided arguments
    filters = []
    if ra is not None:
        filters.append(SkyComponent.ra == ra)
    if dec is not None:
        filters.append(SkyComponent.dec == dec)
    if flux_wide is not None:
        filters.append(SkyComponent.i_pol >= flux_wide)

    # Combine filters using 'and_' if any filters are present
    if filters:
        query = query.filter(and_(*filters))

    return query.all()
